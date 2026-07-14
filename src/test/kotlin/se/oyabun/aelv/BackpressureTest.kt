/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.aelv

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Backpressure correctness tests against the Reactive Streams specification.
 *
 * Each test states the RS rule it verifies and the precise invariant being checked.
 * Tests are expected to FAIL against the current implementation.
 */
class BackpressureTest {

    // -------------------------------------------------------------------------
    // RS 1.1: The total number of onNext signals MUST be ≤ total requested.
    // RS 1.8: After cancel(), the Subscriber MUST eventually stop being signalled.
    //
    // take(n) must stop the upstream after exactly n items. Currently it iterates
    // the entire source and silently discards items past n — violating both rules:
    // items are produced beyond what was ever requested by the downstream, and the
    // upstream is not stopped after the subscription is effectively complete.
    // -------------------------------------------------------------------------

    @Test
    fun `take — upstream produces exactly n items, no more`() = runTest {
        val produced = AtomicLong(0)

        val source = Many.generate<Int> { emit ->
            for (i in 0 until 10_000) {
                produced.incrementAndGet()
                if (emit(Signal.Upstream.Next(i)) == Signal.Downstream.Cancel) return@generate
            }
            emit(Signal.Upstream.Complete)
        }

        val result = source.take(3).toList().await()

        assertIs<Success<List<Int>>>(result)
        assertEquals(listOf(0, 1, 2), result.value)
        assertEquals(
            3L,
            produced.get(),
            "RS 1.1/1.8: upstream produced ${produced.get()} items after take(3), must be exactly 3"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.1 / RS 1.8 — same violation, different operator.
    //
    // takeWhile must stop the upstream the moment the predicate first returns
    // false. Currently the source continues to be called for every remaining item.
    // Allow one extra item (the item that triggered false) as an implementation
    // may need to observe it before stopping, but no more.
    // -------------------------------------------------------------------------

    @Test
    fun `takeWhile — upstream stops at first item that fails the predicate`() = runTest {
        val produced = AtomicLong(0)

        val source = Many.generate<Int> { emit ->
            for (i in 0 until 10_000) {
                produced.incrementAndGet()
                if (emit(Signal.Upstream.Next(i)) == Signal.Downstream.Cancel) return@generate
            }
            emit(Signal.Upstream.Complete)
        }

        val result = source.takeWhile { it < 3 }.toList().await()

        assertIs<Success<List<Int>>>(result)
        assertEquals(listOf(0, 1, 2), result.value)
        assertTrue(
            produced.get() <= 4L,
            "RS 1.1/1.8: upstream produced ${produced.get()} items after takeWhile { < 3 }, must be ≤ 4"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.3: onSubscribe, onNext, onError and onComplete MUST be signalled
    // serially — there MUST be a happens-before relationship between each signal.
    //
    // flatMap with concurrency > 1 launches multiple child coroutines that all
    // share the same emit lambda. Those coroutines execute in parallel on
    // Dispatchers.Default and call emit (→ onNext) concurrently, with no
    // serialisation between them.
    //
    // We instrument the Subscriber directly: track in-flight onNext calls with
    // an AtomicInteger. A correct implementation must never exceed 1 concurrent
    // in-flight call. We widen the race window by holding inside onNext briefly.
    // -------------------------------------------------------------------------

    @Test
    fun `flatMap — onNext is never called concurrently (RS 1_3)`() = runTest {
        val inFlight = AtomicInteger(0)
        val maxObserved = AtomicInteger(0)

        withContext(Dispatchers.Default) {
            val stream: Many<Int> = Many.items(1, 2, 3, 4, 5, 6, 7, 8)
                .flatMap<Int, Int>(concurrency = 4) { item: Int ->
                    Many.generate { emit ->
                        kotlinx.coroutines.yield()
                        emit(Signal.Upstream.Next(item))
                        emit(Signal.Upstream.Complete)
                    }
                }
                .doOnNext {
                    val n = inFlight.incrementAndGet()
                    maxObserved.updateAndGet { prev -> if (n > prev) n else prev }
                    Thread.sleep(2)
                    inFlight.decrementAndGet()
                }
            stream.toList().await()
        }

        assertEquals(
            1,
            maxObserved.get(),
            "RS 1.3: onNext called with ${maxObserved.get()} concurrent in-flight calls, must be exactly 1"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.1: onNext count ≤ requested — zip must stop producing once the shorter
    // stream is exhausted.
    //
    // zip uses two producer coroutines inside coroutineScope. When the consumer
    // loop breaks (shorter stream exhausted), the longer producer is still blocked
    // on channelB.send(). coroutineScope waits for ALL children to complete — the
    // blocked producer never completes — deadlock.
    //
    // The longer source must be larger than Channel.BUFFERED capacity (64) so
    // the producer actually blocks on send() when the consumer stops reading.
    //
    // runBlocking gives a real wall-clock timeout. A virtual-time timeout via
    // runTest would not interrupt a coroutine blocked on a channel send.
    // -------------------------------------------------------------------------

    @Test
    fun `zip — completes when shorter source is exhausted, no deadlock`() {
        runBlocking(Dispatchers.Default) {
            val result = withTimeout(3.seconds) {
                zip(
                    Many.items(1, 2),
                    Many.from((1..200).map { "x$it" }),
                ) { n, s -> "$n$s" }
                    .toList()
                    .await()
            }

            assertIs<Success<List<String>>>(result)
            assertEquals(
                listOf("1x1", "2x2"),
                result.value,
                "zip must emit exactly as many pairs as the shorter source has elements"
            )
        }
    }

    // -------------------------------------------------------------------------
    // RS 1.1: Only as many items as requested must ever be in-flight at once.
    //
    // A slow subscriber that requests one item at a time must not cause the
    // producer to buffer more than one item. We track peak concurrent in-flight
    // (produced but not yet consumed) items using an AtomicInteger and assert it
    // never exceeds 1.
    // -------------------------------------------------------------------------

    @Test
    fun `Many — in-flight item count never exceeds demand (1 at a time)`() = runTest {
        val inFlight = AtomicInteger(0)
        val peakInFlight = AtomicInteger(0)

        val source = Many.generate<Int> { emit ->
            for (i in 0 until 1_000) {
                inFlight.incrementAndGet().also { n ->
                    peakInFlight.updateAndGet { prev -> if (n > prev) n else prev }
                }
                if (emit(Signal.Upstream.Next(i)) == Signal.Downstream.Cancel) return@generate
            }
            emit(Signal.Upstream.Complete)
        }

        source
            .doOnNext { inFlight.decrementAndGet() }
            .toList()
            .await()

        assertEquals(
            1,
            peakInFlight.get(),
            "RS 1.1: peak in-flight was ${peakInFlight.get()}, must be exactly 1 for a synchronous single-demand subscriber"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.1: buffer(n) must hold at most n items at a time in memory.
    //
    // We track peak concurrent items held inside the buffer by counting produced
    // vs consumed items. The delta must never exceed the declared buffer size.
    // -------------------------------------------------------------------------

    @Test
    fun `buffer — never holds more than declared size items in memory`() = runTest {
        val bufferSize = 8
        val produced = AtomicInteger(0)
        val consumed = AtomicInteger(0)
        val peakBuffered = AtomicInteger(0)

        Many.generate<Int> { emit ->
            for (i in 0 until 200) {
                produced.incrementAndGet()
                peakBuffered.updateAndGet { prev ->
                    val current = produced.get() - consumed.get()
                    if (current > prev) current else prev
                }
                if (emit(Signal.Upstream.Next(i)) == Signal.Downstream.Cancel) return@generate
            }
            emit(Signal.Upstream.Complete)
        }
            .buffer(bufferSize)
            .doOnNext { consumed.addAndGet(it.size) }
            .toList()
            .await()

        assertTrue(
            peakBuffered.get() <= bufferSize + 1, // +1 for the item in transit
            "buffer($bufferSize): peak buffered was ${peakBuffered.get()}, must be ≤ ${bufferSize + 1}"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.1: skip(n) must not buffer skipped items — it must signal Cancel after
    // consuming exactly n items, not collect all items and discard later.
    // -------------------------------------------------------------------------

    @Test
    fun `skip — upstream produces only n + demanded items, no full materialization`() = runTest {
        val produced = AtomicLong(0)
        val skip = 5L
        val take = 3L

        val source = Many.generate<Int> { emit ->
            for (i in 0 until 10_000) {
                produced.incrementAndGet()
                if (emit(Signal.Upstream.Next(i)) == Signal.Downstream.Cancel) return@generate
            }
            emit(Signal.Upstream.Complete)
        }

        source.skip(skip).take(take).toList().await()

        assertEquals(
            skip + take,
            produced.get(),
            "skip($skip).take($take): upstream produced ${produced.get()}, must be exactly ${skip + take}"
        )
    }

    // -------------------------------------------------------------------------
    // RS 1.3: signals MUST be serial.
    //
    // combineLatest runs two child coroutines on Dispatchers.Default. They share
    // plain `var latestA` and `var latestB` — no @Volatile, no synchronisation.
    // Each coroutine reads the other's var and calls emit concurrently.
    //
    // Two violations:
    //   a) Concurrent emit calls — RS 1.3 serial requirement broken.
    //   b) Stale read of Unset sentinel: a coroutine reads latestB === Unset
    //      even after the other coroutine has written a real value, due to the
    //      missing memory barrier. A subsequent cast of Unset to B throws
    //      ClassCastException, which propagates as UpstreamErrorException and
    //      becomes Either.Left from toList().await().
    //
    // On x86 with a modern JIT the memory visibility failure is uncommon because
    // the hardware memory model is already strong. This test catches it when it
    // occurs; the bug is definitively present in the source regardless. A
    // reliable deterministic catch requires JVM tooling (-XX:+StressLCM or TSan).
    //
    // Any Either.Left from toList().await() is a definitive failure.
    // -------------------------------------------------------------------------

    @Test
    fun `combineLatest — shared state is safely published across coroutines (RS 1_3)`() {
        var failIteration = -1
        var failResult: Throwable? = null

        runBlocking(Dispatchers.Default) {
            for (i in 0 until 100) {
                val n = 200
                val outcome = combineLatest(
                    Many.from((1..n).toList()),
                    Many.from((1..n).toList()),
                ) { a, b ->
                    // Unset cast to Int throws ClassCastException (stale read).
                    // Out-of-range sum is a logical torn-read indicator.
                    val sum = a + b
                    check(sum in 2..(2 * n)) {
                        "torn read: a=$a b=$b sum=$sum outside [2, ${2 * n}]"
                    }
                    sum
                }
                    .toList()
                    .await()

                if (outcome is Failure) {
                    failIteration = i
                    failResult = outcome.value
                    break
                }
            }
        }

        assertEquals(
            -1,
            failIteration,
            "RS 1.3: combineLatest produced Either.Left on iteration $failIteration — " +
                "torn read or concurrent emit: ${failResult?.message}"
        )
    }
}
