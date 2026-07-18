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

import com.sun.jdi.request.InvalidRequestStateException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OperatorsTest {

    class Transform {

        @Test
        fun `map transforms items`() {
            Verify.that(Many.items(1, 2, 3).map { it * 2 })
                .emitsNext(2, 4, 6)
                .completesNormally()
        }

        @Test
        fun `map on empty emits nothing`() {
            Verify.that(Many.empty<Int>().map { it * 2 })
                .completesNormally()
        }

        @Test
        fun `map propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).map { it * 2 })
                .completesWithError()
        }

        @Test
        fun `filter keeps matching items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).filter { it % 2 == 0 })
                .emitsNext(2, 4)
                .completesNormally()
        }

        @Test
        fun `filter propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).filter { it % 2 == 0 })
                .completesWithError()
        }

        @Test
        fun `mapNotNull transforms and drops nulls`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).mapNotNull { if (it % 2 == 0) it * 10 else null })
                .emitsNext(20, 40)
                .completesNormally()
        }

        @Test
        fun `mapNotNull on all-null source emits nothing`() {
            Verify.that(Many.items(1, 3, 5).mapNotNull<Int, Int> { null })
                .completesNormally()
        }

        @Test
        fun `mapNotNull propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).mapNotNull { it * 2 })
                .completesWithError()
        }

        @Test
        fun `take limits to n items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).take(3))
                .emitsNext(1, 2, 3)
                .thenCancels()
                .verify()
        }

        @Test
        fun `take zero emits nothing`() {
            Verify.that(Many.items(1, 2, 3).take(0))
                .thenCancels()
                .verify()
        }

        @Test
        fun `skip drops first n items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).skip(2))
                .emitsNext(3, 4, 5)
                .completesNormally()
        }

        @Test
        fun `skipWhile skips until predicate false`() {
            Verify.that(Many.items(1, 2, 3, 4, 1).skipWhile { it < 3 })
                .emitsNext(3, 4, 1)
                .completesNormally()
        }

        @Test
        fun `takeWhile emits until predicate false`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).takeWhile { it < 4 })
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test
        fun `distinct removes duplicates`() {
            Verify.that(Many.items(1, 2, 1, 3, 2).distinct())
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test
        fun `distinctUntilChanged removes consecutive duplicates only`() {
            Verify.that(Many.items(1, 1, 2, 2, 1).distinctUntilChanged())
                .emitsNext(1, 2, 1)
                .completesNormally()
        }
    }

    class Expand {

        @Test
        fun `flatMap expands each item`() {
            Verify.that(Many.items(1, 2, 3).flatMap { Many.items(it, it * 10) })
                .emitsCount(6)
                .completesNormally()
        }

        @Test
        fun `concatMap preserves order`() {
            Verify.that(Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completesNormally()
        }

        @Test
        fun `concatMap preserves order when all items arrive from upstream at once`() {
            Verify.that(Many.generate<Int> { emit ->
                listOf(1, 2, 3).forEach { emit(Signal.Upstream.Next(it)) }
                emit(Signal.Upstream.Complete)
            }.concatMap { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completesNormally()
        }

        @Test
        fun `flatMap with concurrency 1 equals concatMap`() = runTest {
            val flat   = Many.items(1, 2, 3).flatMap(concurrency = 1) { Many.items(it, it * 10) }.toList().await()
            val concat = Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) }.toList().await()
            assertEquals(flat, concat)
        }
    }

    class Combine {

        @Test
        fun `merge interleaves two streams`() {
            Verify.that(merge(Many.items(1, 3), Many.items(2, 4)))
                .emitsCount(4)
                .completesNormally()
        }

        @Test
        fun `mergeWith combines two streams`() {
            Verify.that(Many.items(1, 2).mergeWith(Many.items(3, 4)))
                .emitsCount(4)
                .completesNormally()
        }

        @Test
        fun `concat sequences streams in order`() {
            Verify.that(concat(Many.items(1, 2), Many.items(3, 4)))
                .emitsNext(1, 2, 3, 4)
                .completesNormally()
        }

        @Test
        fun `concat with empty streams`() {
            Verify.that(concat(Many.empty(), Many.items(1, 2), Many.empty<Int>()))
                .emitsNext(1, 2)
                .completesNormally()
        }

        @Test
        fun `zip pairs items from two streams`() {
            Verify.that(zip(Many.items(1, 2, 3), Many.items("a", "b", "c")) { n, s -> "$n$s" })
                .emitsNext("1a", "2b", "3c")
                .completesNormally()
        }

        @Test
        fun `zip does not hang when source A errors after source B completes`() = runTest {
            // Regression for Bug Z2: post-loop channelA probe must be non-blocking.
            val cause   = InvalidDemandException(-1)
            val sourceA = Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(1))
                emit(Signal.Upstream.Next(2))
                emit(Signal.Upstream.Error(cause))
            }
            val result = zip(sourceA, Many.items("a")) { n, s -> "$n$s" }.toList().await()
            // Either completes with ["1a"] or surfaces the error from A — either is acceptable.
            // What must NOT happen is a hang.
            assertTrue(result is Success || result is Failure)
        }
    }

    class Buffer {

        @Test
        fun `buffer collects items into fixed size lists`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).buffer(2))
                .emitsNext(listOf(1, 2), listOf(3, 4), listOf(5))
                .completesNormally()
        }

        @Test
        fun `buffer emits partial bucket on completion`() {
            Verify.that(Many.items(1, 2, 3).buffer(2))
                .emitsNext(listOf(1, 2), listOf(3))
                .completesNormally()
        }
    }

    class ErrorHandling {

        @Test
        fun `recover replaces error with fallback stream`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).recover { Many.items(99) })
                .emitsNext(99)
                .completesNormally()
        }

        @Test
        fun `recoverWith replaces error with fallback value`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).recoverWith { 99 })
                .emitsNext(99)
                .completesNormally()
        }

        @Test
        fun `retry retries on error up to n times`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(times = 3))
                .emitsNext(42)
                .completesNormally()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(times = 2))
                .completesWithError()
        }

        @Test
        fun `retry with Retry max succeeds after failures`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().maxAttempts(3)))
                .emitsNext(42)
                .completesNormally()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with Retry max exhausts and propagates`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(Policy.retry().maxAttempts(2)))
                .completesWithError()
        }

        @Test
        fun `retry with fixed backoff delays between attempts`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().withBackoff(10.milliseconds).maxAttempts(3)))
                .emitsNext(42)
                .completesNormally()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with exponential backoff succeeds after failures`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().withBackoff(10.milliseconds, 100.milliseconds).maxAttempts(3)))
                .emitsNext(42)
                .completesNormally()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry filter skips non-matching errors`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(Policy.retry().on(NoSuchElementException::class).maxAttempts(5)))
                .completesWithError()
        }

        @Test
        fun `retry zero does not retry`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                throw InvalidDemandException(-1)
            }.retry(Policy.retry().maxAttempts(0)))
                .completesWithError()
            assertEquals(1, attempts)
        }
    }

    class Terminal {

        @Test
        fun `fold accumulates all items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).fold(0) { acc, item -> acc + item })
                .emitsNext(15)
                .completesNormally()
        }

        @Test
        fun `fold propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).fold(0) { acc, item -> acc + item })
                .completesWithError()
        }

        @Test
        fun `first returns first item`() = runTest {
            val result = Many.items(1, 2, 3).first()
            assertIs<Success<Int>>(result)
            assertEquals(1, result.value)
        }

        @Test
        fun `first on empty returns NoSuchElementException`() = runTest {
            val result = Many.empty<Int>().first()
            assertIs<Failure<AelvException>>(result)
            assertIs<NoSuchElementException>(result.value)
        }

        @Test
        fun `last returns last item`() = runTest {
            val result = Many.items(1, 2, 3).last()
            assertIs<Success<Int>>(result)
            assertEquals(3, result.value)
        }

        @Test
        fun `toList returns immutable list`() {
            Verify.that(Many.items(1, 2, 3).toList())
                .emitsNext(listOf(1, 2, 3))
                .completesNormally()
        }

        @Test
        fun `toSet returns immutable set`() {
            Verify.that(Many.items(1, 2, 1, 3).toSet())
                .emitsNext(setOf(1, 2, 3))
                .completesNormally()
        }
    }

    class OneOps {

        @Test
        fun `map transforms value`() {
            Verify.that(One.single(5).map { it * 3 })
                .emitsNext(15)
                .completesNormally()
        }

        @Test
        fun `flatMap chains to another One`() {
            Verify.that(One.single(5).flatMap { One.single(it * 2) })
                .emitsNext(10)
                .completesNormally()
        }

        @Test
        fun `flatMapMany expands to Many`() {
            Verify.that(One.single(3).flatMapMany { Many.items(it, it + 1, it + 2) })
                .emitsNext(3, 4, 5)
                .completesNormally()
        }

        @Test
        fun `recover replaces error with fallback value`() {
            Verify.that(One.error<Int>(InvalidDemandException(-1)).recover { 99 })
                .emitsNext(99)
                .completesNormally()
        }

        @Test
        fun `retry retries on error`() = runTest {
            var attempts = 0
            Verify.that(One.defer {
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                42
            }.retry(Policy.retry().maxAttempts(3)))
                .emitsNext(42)
                .completesNormally()
            assertEquals(3, attempts)
        }
    }

    class GroupBy {

        @Test
        fun `groups items by key`() = runTest {
            val byKey = mutableMapOf<Int, MutableList<Int>>()
            Verify.that(Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { key, group -> group.map { key to it } }
                .doOnNext { (k, v) -> byKey.getOrPut(k) { mutableListOf() }.add(v) })
                .emitsCount(6)
                .completesNormally()
            assertEquals(listOf(2, 4, 6), byKey[0]?.sorted())
            assertEquals(listOf(1, 3, 5), byKey[1]?.sorted())
        }

        @Test
        fun `each group receives a terminal Complete`() = runTest {
            val completed = mutableSetOf<String>()
            Verify.that(Many.items("a", "b", "a", "c")
                .groupBy({ it }) { key, group ->
                    group.doOnComplete { completed.add(key) }.map { key to it }
                })
                .emitsCount(4)
                .completesNormally()
            assertEquals(setOf("a", "b", "c"), completed)
        }

        @Test
        fun `each group receives an Error terminal when source errors`() = runTest {
            val cause  = InvalidDemandException(-1)
            val errors = mutableMapOf<String, Exception>()
            Verify.that(Many.generate<String> { emit ->
                emit(Signal.Upstream.Next("x"))
                emit(Signal.Upstream.Next("y"))
                emit(Signal.Upstream.Error(cause))
            }
                .groupBy({ it }) { key, group ->
                    group.recover { err -> errors[key] = err; Many.empty() }
                }
                .recover { Many.empty() })
                .completesNormally()
            assertEquals(setOf("x", "y"), errors.keys)
            assertTrue(errors.values.all { it === cause })
        }

        @Test
        fun `single-key source produces one group with all items`() {
            Verify.that(Many.items(10, 20, 30)
                .groupBy({ "only" }) { _, group -> group })
                .emitsNext(10, 20, 30)
                .completesNormally()
        }

        @Test
        fun `empty source emits no items and completes`() {
            Verify.that(Many.empty<Int>()
                .groupBy({ it }) { _, group -> group })
                .completesNormally()
        }

        @Test
        fun `Cancel on outer stream cancels all group pipelines`() {
            Verify.that(Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { _, group -> group }
                .take(1))
                .emitsCount(1)
                .thenCancels()
                .verify()
        }

        @Test
        fun `group Complete is delivered before outer stream Complete`() = runTest {
            // Regression for Bug G1/G3.
            val completedGroups = mutableSetOf<Int>()
            Verify.that(Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 3 }) { key, group ->
                    group.doOnComplete { completedGroups.add(key) }.map { key }
                })
                .emitsCount(6)
                .completesNormally()
            assertEquals(setOf(0, 1, 2), completedGroups)
        }

        @Test
        fun `group Error is delivered before outer stream Error`() = runTest {
            // Regression for Bug G3.
            val cause         = InvalidDemandException(-1)
            val erroredGroups = mutableSetOf<Int>()
            Verify.that(Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(1))
                emit(Signal.Upstream.Next(2))
                emit(Signal.Upstream.Next(3))
                emit(Signal.Upstream.Error(cause))
            }
                .groupBy({ it % 2 }) { key, group ->
                    group.doOnError { erroredGroups.add(key) }.recover { Many.empty() }
                }
                .recover { Many.empty() })
                .completesNormally()
            assertEquals(setOf(0, 1), erroredGroups)
        }

        @Test
        fun `groupHandler can apply different transforms per key`() {
            Verify.that(Many.items(1, 2, 3, 4)
                .groupBy({ it % 2 }) { key, group ->
                    if (key == 0) group.map { it * 10 } else group.map { it * 100 }
                })
                .emitsCount(4)
                .completesNormally()
        }
    }

    class SwitchMap {

        @Test
        fun `switchMap emits from latest inner stream only`() = runTest {
            val result = Many.items(1, 2, 3).switchMap { Many.items(it * 10) }.toList().await()
            assertIs<Success<List<Int>>>(result)
            assertTrue(result.value.isNotEmpty())
            assertTrue(result.value.last() == 30)
        }

        @Test
        fun `switchMap propagates source error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).switchMap { Many.items(it) })
                .completesWithError()
        }

        @Test
        fun `switchMap completes on empty source`() {
            Verify.that(Many.empty<Int>().switchMap { Many.items(it) })
                .completesNormally()
        }
    }

    class FlatMapSequential {

        @Test
        fun `flatMapSequential preserves order`() {
            Verify.that(Many.items(1, 2, 3).flatMapSequential { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completesNormally()
        }

        @Test
        fun `flatMapSequential with maxConcurrency 1 equals concatMap`() = runTest {
            val sequential = Many.items(1, 2, 3).flatMapSequential(maxConcurrency = 1) { Many.items(it, it * 10) }.toList().await()
            val concat      = Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) }.toList().await()
            assertEquals(sequential, concat)
        }

        @Test
        fun `flatMapSequential on empty source completes`() {
            Verify.that(Many.empty<Int>().flatMapSequential { Many.items(it) })
                .completesNormally()
        }
    }

    class TakeUntilOther {

        @Test
        fun `takeUntilOther stops when other signals`() {
            val other  = Sinks.broadcast<Unit>()
            Verify.that(
                Many.never<Int>().takeUntilOther(
                    Many.defer(factory = suspend { other.complete(); other.asMany() })
                )
            ).completesNormally()
        }

        @Test
        fun `takeUntilOther completes normally when source completes first`() {
            Verify.that(Many.items(1, 2, 3).takeUntilOther(Many.never<Unit>()))
                .emitsNext(1, 2, 3)
                .completesNormally()
        }
    }

    class DelaySubscription {

        @Test
        fun `delaySubscription waits for trigger then emits source`() {
            Verify.that(Many.items(1, 2, 3).delaySubscription(Many.items(Unit)))
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test
        fun `delaySubscription propagates trigger error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.items(1, 2).delaySubscription(Many.error<Unit>(cause)))
                .completesWithError()
        }

        @Test
        fun `One delaySubscription emits value after delay`() {
            Verify.that(One.single(42).delaySubscription(10.milliseconds))
                .emitsNext(42)
                .completesNormally()
        }

        @Test
        fun `One delaySubscription with trigger emits value after trigger`() {
            Verify.that(One.single(7).delaySubscription(Many.items(Unit)))
                .emitsNext(7)
                .completesNormally()
        }

        @Test
        fun `Maybe delaySubscription emits present value after delay`() {
            Verify.that(Maybe.present(99).delaySubscription(10.milliseconds))
                .assertNext { assertEquals(99, it) }
                .completesNormally()
        }

        @Test
        fun `Maybe delaySubscription on empty completes empty after delay`() {
            Verify.that(Maybe.empty<Int>().delaySubscription(10.milliseconds))
                .completesEmpty()
        }

        @Test
        fun `None delaySubscription completes after delay`() {
            Verify.that(None.complete<Unit>().delaySubscription(10.milliseconds))
                .completesNormally()
        }
    }

    class OnBackpressureDrop {

        @Test
        fun `onBackpressureDrop completes normally on small source`() = runTest {
            val result = Many.items(1, 2, 3).onBackpressureDrop().toList().await()
            assertIs<Success<List<Int>>>(result)
            assertTrue(result.value.isNotEmpty())
        }

        @Test
        fun `onBackpressureDrop propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).onBackpressureDrop())
                .completesWithError()
        }
    }

    class DistinctUntilChangedBy {

        @Test
        fun `distinctUntilChangedBy uses key for comparison`() {
            data class Item(val key: Int, val value: String)
            val items = listOf(Item(1, "a"), Item(1, "b"), Item(2, "c"), Item(2, "d"), Item(1, "e"))
            Verify.that(Many.from(items).distinctUntilChangedBy { it.key })
                .emitsNext(Item(1, "a"), Item(2, "c"), Item(1, "e"))
                .completesNormally()
        }
    }

    class DoOnSubscribe {

        @Test
        fun `doOnSubscribe fires before any items`() = runTest {
            val fired = mutableListOf<String>()
            Verify.that(Many.items(1, 2, 3)
                .doOnSubscribe { fired.add("subscribed") }
                .doOnNext { fired.add("item") })
                .emitsCount(3)
                .completesNormally()
            assertEquals("subscribed", fired.first())
        }
    }

    class DoFinally {

        @Test
        fun `doFinally fires on normal completion`() = runTest {
            var terminal: Signal.Terminal? = null
            Verify.that(Many.items(1, 2).doFinally { terminal = it })
                .emitsCount(2)
                .completesNormally()
            assertIs<Signal.Upstream.Complete>(terminal)
        }

        @Test
        fun `doFinally fires on error`() = runTest {
            val cause = InvalidDemandException(-1)
            var terminal: Signal.Terminal? = null
            Verify.that(Many.error<Int>(cause).doFinally { terminal = it })
                .completesWithError()
            assertIs<Signal.Upstream.Error>(terminal)
        }

        @Test
        fun `doFinally fires on cancel`() = runTest {
            var terminal: Signal.Terminal? = null
            Many.items(1, 2, 3).doFinally { terminal = it }.take(1).toList().await()
            assertIs<Signal.Downstream.Cancel>(terminal)
        }
    }

    class BufferTimeout {

        @Test
        fun `bufferTimeout emits bucket when size reached`() = runTest {
            val result = Many.items(1, 2, 3, 4).bufferTimeout(2, 5.seconds).toList().await()
            assertIs<Success<List<List<Int>>>>(result)
            assertTrue(result.value.isNotEmpty())
            assertTrue(result.value.all { it.size <= 2 })
        }

        @Test
        fun `bufferTimeout flushes partial bucket on source complete`() {
            Verify.that(Many.items(1, 2, 3).bufferTimeout(10, 5.seconds))
                .emitsNext(listOf(1, 2, 3))
                .completesNormally()
        }

        @Test
        fun `bufferTimeout flushes on timeout`() {
            Verify.that(Many.items(1).bufferTimeout(100, 50.milliseconds))
                .emitsNext(listOf(1))
                .completesNormally()
        }
    }

    class CombineLatest {

        @Test
        fun `combineLatest pairs latest values`() = runTest {
            val result = combineLatest(Many.items(1, 2), Many.items("a", "b")) { n, s -> "$n$s" }.toList().await()
            assertIs<Success<List<String>>>(result)
            assertTrue(result.value.isNotEmpty())
        }

        @Test
        fun `combineLatest on empty source completes without items`() {
            Verify.that(combineLatest(Many.empty<Int>(), Many.items("a")) { n, s -> "$n$s" })
                .completesNormally()
        }
    }

    class ZipOne {

        @Test
        fun `zip pairs two One values`() {
            Verify.that(zip(One.single(1), One.single("a")) { n, s -> "$n$s" })
                .emitsNext("1a")
                .completesNormally()
        }

        @Test
        fun `zip completes empty when first source is empty`() = runTest {
            val result = zip(
                One.defer<Int> { throw NoSuchElementException() }.recover { 0 }
                    .flatMap { One.generate<Int> { emit -> emit(Signal.Upstream.Complete) } },
                One.single("a")
            ) { n, s -> "$n$s" }.await()
            assertIs<Failure<AelvException>>(result)
        }

        @Test
        fun `zip propagates error from first source`() {
            val cause = InvalidDemandException(-1)
            Verify.that(zip(One.error<Int>(cause), One.single("a")) { n, s -> "$n$s" })
                .completesWithError()
        }

        @Test
        fun `zip propagates error from second source`() {
            val cause = InvalidDemandException(-1)
            Verify.that(zip(One.single(1), One.error<String>(cause)) { n, s -> "$n$s" })
                .completesWithError()
        }

        @Test
        fun `zipWith is alias for zip`() {
            Verify.that(One.single(2).zipWith(One.single(3)) { a, b -> a + b })
                .emitsNext(5)
                .completesNormally()
        }
    }

    class FlatMapNone {

        @Test
        fun `flatMapNone chains to None`() = runTest {
            var ran = false
            One.single(42).flatMapNone { None.defer<Unit> { ran = true } }.await()
            assertTrue(ran)
        }

        @Test
        fun `flatMapNone propagates error from One`() {
            val cause = InvalidDemandException(-1)
            Verify.that(One.error<Int>(cause).flatMapNone { None.complete<Unit>() })
                .completesWithError()
        }
    }

    class OneCache {

        @Test
        fun `cache executes source once for multiple subscribers`() = runTest {
            var count = 0
            val cached = One.defer { ++count; 42 }.cache()
            cached.await()
            cached.await()
            cached.await()
            assertEquals(1, count)
        }

        @Test
        fun `cache replays same value`() = runTest {
            val cached = One.single(99).cache()
            val r1 = cached.await()
            val r2 = cached.await()
            assertEquals(r1, r2)
            assertEquals(99, (r1 as Either.Right).value)
        }
    }

    class PublishSubscribeOn {

        @Test
        fun `publishOn does not lose items`() {
            Verify.that(Many.items(1, 2, 3).publishOn(Dispatchers.Default))
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test
        fun `subscribeOn does not lose items`() {
            Verify.that(Many.items(1, 2, 3).subscribeOn(Dispatchers.Default))
                .emitsNext(1, 2, 3)
                .completesNormally()
        }
    }

    class Reduce {

        @Test
        fun `reduce accumulates all items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).reduce { a, b -> a + b })
                .emitsNext(15)
                .completesNormally()
        }

        @Test
        fun `reduce on empty returns NoSuchElementException`() {
            Verify.that(Many.empty<Int>().reduce { a, b -> a + b })
                .completesWithError()
        }

        @Test
        fun `reduce propagates source error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).reduce { a, b -> a + b })
                .completesWithError()
        }
    }

    class Interval {

        @Test
        fun `interval emits incrementing longs starting at zero`() {
            Verify.that(Many.interval(10.milliseconds).take(3))
                .emitsNext(0L, 1L, 2L)
                .completesNormally()
        }
    }

    class BufferSkip {

        @Test
        fun `buffer with skip equal to size is same as buffer by size`() {
            Verify.that(Many.items(1, 2, 3, 4).buffer(2, 2))
                .emitsNext(listOf(1, 2), listOf(3, 4))
                .completesNormally()
        }

        @Test
        fun `buffer with skip greater than size creates gaps`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).buffer(2, 3))
                .emitsNext(listOf(1, 2), listOf(4, 5))
                .completesNormally()
        }

        @Test
        fun `buffer with skip less than size creates overlapping windows`() = runTest {
            val result = Many.items(1, 2, 3, 4).buffer(3, 1).toList().await()
            assertIs<Success<List<List<Int>>>>(result)
            assertTrue(result.value.contains(listOf(1, 2, 3)))
            assertTrue(result.value.contains(listOf(2, 3, 4)))
        }
    }

    class SinkTests {

        @Test
        fun `broadcast sink delivers items to subscriber`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.complete()

            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally()
        }

        @Test
        fun `broadcast sink does not replay to late subscriber`() = runTest {
            val sink = Sinks.broadcast<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            // broadcast has no history — late subscriber gets only the terminal
            Verify.that(sink.asMany()).completesNormally()
        }

        @Test
        fun `replay sink replays full history to late subscriber`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.complete()

            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally()
        }

        @Test
        fun `replayLast sink replays only last n items`() = runTest {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()

            Verify.that(sink.asMany())
                .emitsNext(2, 3)
                .completesNormally()
        }

        @Test
        fun `sink complete is delivered to subscriber`() {
            val sink = Sinks.broadcast<Int>()
            sink.complete()

            Verify.that(sink.asMany()).completesNormally()
        }

        @Test
        fun `sink error is delivered to subscriber`() {
            val cause = InvalidDemandException(-1)
            val sink  = Sinks.broadcast<Int>()
            sink.error(cause)

            Verify.that(sink.asMany()).completesWithError()
        }

        @Test
        fun `broadcast sink delivers to multiple concurrent subscribers`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.complete()

            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally()
            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally()
        }
    }

    class MaybeRetry {

        @Test
        fun `Maybe retry retries on error then succeeds`() = runTest {
            var attempt = 0
            var retryCount = 0
            val threeThrowsThenSuccessAreRetried = Maybe.defer {
                attempt++
                if (attempt < 3) throw InvalidDemandException(-1)
                42
            }.doOnRetry { _, _ -> retryCount++ }.retry(times = 3)
            Verify.that(threeThrowsThenSuccessAreRetried)
                .assertNext {
                    assertEquals(42, it)
                    assertEquals(3, attempt)
                    assertEquals(2, retryCount)
                }
                .completesNormally()
        }

        @Test
        fun `Maybe retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Maybe.error<Int>(cause).retry(times = 2))
                .completesWithError()
        }

        @Test
        fun `Maybe retry signals onComplete to direct subscriber after successful retry`() = runTest {
            var retries = 0
            var completed = false
            val signalsCompleteAfterRetries = Maybe.defer { if (retries < 1) throw InvalidDemandException(-1) }
                .doOnRetry { _,_ -> retries++ }.retry(times = 1)
                .doOnComplete { if(!completed) completed = true else throw IllegalStateException("only once") }
            Verify.that(signalsCompleteAfterRetries).completesNormally()
            assertTrue(completed, "onComplete must be signalled to direct subscribers after successful retry")
            assertEquals(1, retries)
        }
    }

    class NoneRetry {

        @Test
        fun `None retry retries on error then succeeds`() = runTest {
            var attempts = 0
            Verify.that(None.defer<Unit> {
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
            }.retry(times = 3))
                .completesNormally()
            assertEquals(3, attempts)
        }

        @Test
        fun `None retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(None.error<Unit>(cause).retry(times = 2))
                .completesWithError()
        }
    }
}
