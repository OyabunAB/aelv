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
import kotlinx.coroutines.delay
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
                .completes()
        }

        @Test
        fun `map on empty emits nothing`() {
            Verify.that(Many.empty<Int>().map { it * 2 })
                .completes()
        }

        @Test
        fun `map propagates error`() {
            Verify.that(Many.items(1).map { throw InvalidDemandException(-1) })
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `filter keeps matching items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).filter { it % 2 == 0 })
                .emitsNext(2, 4)
                .completes()
        }

        @Test
        fun `filter propagates error`() {
            Verify.that(Many.items(1).filter { throw InvalidDemandException(-1) })
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `mapNotNull transforms and drops nulls`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).mapNotNull { if (it % 2 == 0) it * 10 else null })
                .emitsNext(20, 40)
                .completes()
        }

        @Test
        fun `mapNotNull on all-null source emits nothing`() {
            Verify.that(Many.items(1, 3, 5).mapNotNull<Int, Int> { null })
                .completes()
        }

        @Test
        fun `mapNotNull propagates error`() {
            Verify.that(Many.items(1).mapNotNull { throw InvalidDemandException(-1) })
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `take limits to n items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).take(3))
                .emitsNext(1, 2, 3)
                .cancels()
        }

        @Test
        fun `take zero emits nothing`() {
            Verify.that(Many.items(1, 2, 3).take(0))
                .cancels()
        }

        @Test
        fun `skip drops first n items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).skip(2))
                .emitsNext(3, 4, 5)
                .completes()
        }

        @Test
        fun `skipWhile skips until predicate false`() {
            Verify.that(Many.items(1, 2, 3, 4, 1).skipWhile { it < 3 })
                .emitsNext(3, 4, 1)
                .completes()
        }

        @Test
        fun `takeWhile emits until predicate false`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).takeWhile { it < 4 })
                .emitsNext(1, 2, 3)
                .completes()
        }

        @Test
        fun `takeWhile signals onComplete exactly once when predicate fails`() {
            var completions = 0
            Verify.that(
                Many.items(1, 2, 3, 4, 5)
                    .takeWhile { it < 3 }
                    .doOnComplete { completions++ }
            )
                .emitsNext(1, 2)
                .completes()
            assertEquals(1, completions, "onComplete must fire exactly once")
        }

        @Test
        fun `distinct removes duplicates`() {
            Verify.that(Many.items(1, 2, 1, 3, 2).distinct())
                .emitsNext(1, 2, 3)
                .completes()
        }

        @Test
        fun `distinctUntilChanged removes consecutive duplicates only`() {
            Verify.that(Many.items(1, 1, 2, 2, 1).distinctUntilChanged())
                .emitsNext(1, 2, 1)
                .completes()
        }
    }

    class Expand {

        @Test
        fun `flatMap expands each item`() {
            val source = Many.items(1, 2, 3)
            val result = source.flatMap { Many.items(it, it * 10) }.toList().map { it.sorted() }
            Verify.that(result).emitsNext(listOf(1, 2, 3, 10, 20, 30)).completes()
        }

        @Test
        fun `concatMap preserves order`() {
            Verify.that(Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completes()
        }

        @Test
        fun `concatMap preserves order when all items arrive from upstream at once`() {
            Verify.that(Many.generate<Int> { emit ->
                listOf(1, 2, 3).forEach { emit(Signal.Upstream.Next(it)) }
                emit(Signal.Upstream.Complete)
            }.concatMap { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completes()
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
            val a      = Many.items(1, 3)
            val b      = Many.items(2, 4)
            val result = merge(a, b).toList().map { it.sorted() }
            Verify.that(result).emitsNext(listOf(1, 2, 3, 4)).completes()
        }

        @Test
        fun `mergeWith combines two streams`() {
            val a      = Many.items(1, 2)
            val b      = Many.items(3, 4)
            val result = a.mergeWith(b).toList().map { it.sorted() }
            Verify.that(result).emitsNext(listOf(1, 2, 3, 4)).completes()
        }

        @Test
        fun `concat sequences streams in order`() {
            val a = Many.items(1, 2)
            val b = Many.items(3, 4)
            Verify.that(concat(a, b)).emitsNext(1, 2, 3, 4).completes()
        }

        @Test
        fun `concat with empty streams`() {
            val source = Many.items(1, 2)
            Verify.that(concat(Many.empty(), source, Many.empty<Int>()))
                .emitsNext(1, 2)
                .completes()
        }

        @Test
        fun `zip pairs items from two streams`() {
            val numbers = Many.items(1, 2, 3)
            val letters = Many.items("a", "b", "c")
            Verify.that(zip(numbers, letters) { n, s -> "$n$s" })
                .emitsNext("1a", "2b", "3c")
                .completes()
        }

        @Test
        fun `zip does not hang when source A errors after source B completes`() {
            val cause   = InvalidDemandException(-1)
            val sourceA = Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(1))
                emit(Signal.Upstream.Next(2))
                emit(Signal.Upstream.Error(cause))
            }
            val sourceB = Many.items("a")
            Verify.that(zip(sourceA, sourceB) { n, s -> "$n$s" })
                .emitsNext("1a")
                .completes()
        }
    }

    class Buffer {

        @Test
        fun `buffer collects items into fixed size lists`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).buffer(2))
                .emitsNext(listOf(1, 2), listOf(3, 4), listOf(5))
                .completes()
        }

        @Test
        fun `buffer emits partial bucket on completion`() {
            Verify.that(Many.items(1, 2, 3).buffer(2))
                .emitsNext(listOf(1, 2), listOf(3))
                .completes()
        }
    }

    class ErrorHandling {

        @Test
        fun `recover replaces error with fallback stream`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).recover { Many.items(99) })
                .emitsNext(99)
                .completes()
        }

        @Test
        fun `recoverWith replaces error with fallback value`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).recoverWith { 99 })
                .emitsNext(99)
                .completes()
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
                .completes()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(times = 2))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
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
                .completes()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with Retry max exhausts and propagates`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(Policy.retry().maxAttempts(2)))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
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
                .completes()
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
                .completes()
            assertEquals(3, attempts)
        }

        @Test
        fun `retry filter skips non-matching errors`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).retry(Policy.retry().on(NoElementException::class).maxAttempts(5)))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `retry zero does not retry`() = runTest {
            var attempts = 0
            Verify.that(Many.generate<Int> { emit ->
                attempts++
                throw InvalidDemandException(-1)
            }.retry(Policy.retry().maxAttempts(0)))
                .fails()
            assertEquals(1, attempts)
        }
    }

    class Terminal {

        @Test
        fun `fold accumulates all items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).fold(0) { acc, item -> acc + item })
                .emitsNext(15)
                .completes()
        }

        @Test
        fun `fold propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).fold(0) { acc, item -> acc + item })

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `first returns first item`() {
            Verify.that(Many.items(1, 2, 3).first())
                .assertNext { assertEquals(1, it) }
                .completes()
        }

        @Test
        fun `first on empty returns NoElementException`() {
            Verify.that(Many.empty<Int>().first())
                .failsWith<NoElementException>()
        }

        @Test
        fun `last returns last item`() {
            Verify.that(Many.items(1, 2, 3).last())
                .assertNext { assertEquals(3, it) }
                .completes()
        }

        @Test
        fun `toList returns immutable list`() {
            Verify.that(Many.items(1, 2, 3).toList())
                .emitsNext(listOf(1, 2, 3))
                .completes()
        }

        @Test
        fun `toSet returns immutable set`() {
            Verify.that(Many.items(1, 2, 1, 3).toSet())
                .emitsNext(setOf(1, 2, 3))
                .completes()
        }
    }

    class OneOps {

        @Test
        fun `map transforms value`() {
            Verify.that(One.single(5).map { it * 3 })
                .emitsNext(15)
                .completes()
        }

        @Test
        fun `flatMap chains to another One`() {
            Verify.that(One.single(5).flatMap { One.single(it * 2) })
                .emitsNext(10)
                .completes()
        }

        @Test
        fun `flatMapMany expands to Many`() {
            Verify.that(One.single(3).flatMapMany { Many.items(it, it + 1, it + 2) })
                .emitsNext(3, 4, 5)
                .completes()
        }

        @Test
        fun `recover replaces error with fallback value`() {
            Verify.that(One.error<Int>(InvalidDemandException(-1)).recover { One.single(99) })
                .emitsNext(99)
                .completes()
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
                .completes()
            assertEquals(3, attempts)
        }

        @Test
        fun `flatMapMaybe maps value to present Maybe`() {
            Verify.that(One.single(5).flatMapMaybe { Maybe.present(it * 2) })
                .emitsNext(10)
                .completes()
        }

        @Test
        fun `flatMapMaybe maps value to empty Maybe`() {
            Verify.that(One.single(5).flatMapMaybe { Maybe.empty<Int>() })
                .completes()
        }

        @Test
        fun `flatMapMaybe propagates error`() {
            Verify.that(One.error<Int>(InvalidDemandException(-1)).flatMapMaybe { Maybe.present(it) })
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `await with timeout returns value when it arrives in time`() = runTest {
            val result = One.single(42).await(5.seconds)
            assertIs<Success<Int>>(result)
            assertEquals(42, result.value)
        }

        @Test
        fun `await with timeout returns ExceededTimeoutException when source does not emit`() = runTest {
            val result = One.never<Int>().await(10.milliseconds)
            assertIs<Failure<Exception>>(result)
            assertIs<ExceededTimeoutException>(result.value)
        }
    }

    class GroupBy {

        @Test
        fun `groups items by key`() = runTest {
            val byKey = mutableMapOf<Int, MutableList<Int>>()
            val source = Many.items(1, 2, 3, 4, 5, 6)
            Verify.that(source
                .groupBy({ it % 2 }) { key, group -> group.map { key to it } }
                .doOnNext { (k, v) -> byKey.getOrPut(k) { mutableListOf() }.add(v) }
                .toList()
                .map { it.size })
                .emitsNext(6)
                .completes()
            assertEquals(listOf(2, 4, 6), byKey[0]?.sorted())
            assertEquals(listOf(1, 3, 5), byKey[1]?.sorted())
        }

        @Test
        fun `each group receives a terminal Complete`() = runTest {
            val completed = mutableSetOf<String>()
            val source    = Many.items("a", "b", "a", "c")
            Verify.that(source
                .groupBy({ it }) { key, group ->
                    group.doOnComplete { completed.add(key) }.map { key to it }
                }
                .toList()
                .map { it.map { (k, _) -> k }.toSet() })
                .emitsNext(setOf("a", "b", "c"))
                .completes()
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
                .completes()
            assertEquals(setOf("x", "y"), errors.keys)
            assertTrue(errors.values.all { it === cause })
        }

        @Test
        fun `single-key source produces one group with all items`() {
            Verify.that(Many.items(10, 20, 30)
                .groupBy({ "only" }) { _, group -> group })
                .emitsNext(10, 20, 30)
                .completes()
        }

        @Test
        fun `empty source emits no items and completes`() {
            Verify.that(Many.empty<Int>()
                .groupBy({ it }) { _, group -> group })
                .completes()
        }

        @Test
        fun `Cancel on outer stream cancels all group pipelines`() {
            val source = Many.items(1, 2, 3, 4, 5, 6)
            Verify.that(source
                .groupBy({ it % 2 }) { _, group -> group }
                .take(1))
                .emitsCount(1)
                .completes()
        }

        @Test
        fun `group Complete is delivered before outer stream Complete`() = runTest {
            // Regression: group onComplete was fired after the outer stream completed, not before.
            val completedGroups = mutableSetOf<Int>()
            Verify.that(Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 3 }) { key, group ->
                    group.doOnComplete { completedGroups.add(key) }.map { key }
                })
                .emitsCount(6)
                .completes()
            assertEquals(setOf(0, 1, 2), completedGroups)
        }

        @Test
        fun `group Error is delivered before outer stream Error`() = runTest {
            // Regression: group onError was fired after the outer stream errored, not before.
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
                .completes()
            assertEquals(setOf(0, 1), erroredGroups)
        }

        @Test
        fun `groupHandler can apply different transforms per key`() {
            Verify.that(
                Many.items(1, 2, 3, 4)
                    .groupBy({ it % 2 }) { key, group ->
                        if (key == 0) group.map { it * 10 } else group.map { it * 100 }
                    }
                    .toList()
                    .map { it.sorted() }
            )
                .emitsNext(listOf(20, 40, 100, 300))
                .completes()
        }
    }

    class SwitchMap {

        @Test
        fun `switchMap cancels in-flight inner when new outer item arrives`() {
            val outerSink = Sinks.unicast<Int>()
            // Driver emits outer item 1, waits for inner1 to start blocking, then emits outer item 2.
            // If switchMap correctly cancels inner1 (Many.never), the stream emits [20] and completes.
            // If switchMap fails to cancel inner1, the stream never completes and the test times out.
            val driver: Many<Int> = None.defer<Int> {
                outerSink.emit(1)
                delay(50.milliseconds)
                outerSink.emit(2)
                outerSink.complete()
            }.toMany()
            Verify.that(
                merge(
                    outerSink.asMany().switchMap { outer ->
                        if (outer == 1) Many.never() else Many.items(outer * 10)
                    },
                    driver,
                ),
                Dispatchers.Default,
            )
                .emitsNext(20)
                .completes(within = 5.seconds)
        }

        @Test
        fun `switchMap propagates source error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).switchMap { Many.items(it) })
                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `switchMap completes on empty source`() {
            Verify.that(Many.empty<Int>().switchMap { Many.items(it) })
                .completes()
        }
    }

    class FlatMapSequential {

        @Test
        fun `flatMapSequential preserves order`() {
            Verify.that(Many.items(1, 2, 3).flatMapSequential { Many.items(it, it * 10) })
                .emitsNext(1, 10, 2, 20, 3, 30)
                .completes()
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
                .completes()
        }

        @Test
        fun `flatMapSequential does not emit Complete after downstream cancel`() {
            var completeCount = 0
            Verify.that(
                Many.items(1, 2, 3, 4, 5)
                    .flatMapSequential { Many.items(it) }
                    .take(2)
                    .doOnComplete { completeCount++ }
            )
                .emitsNext(1, 2)
                .completes()
            assertEquals(1, completeCount, "Complete must be signalled exactly once")
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
            ).completes()
        }

        @Test
        fun `takeUntilOther completes normally when source completes first`() {
            Verify.that(Many.items(1, 2, 3).takeUntilOther(Many.never<Unit>()))
                .emitsNext(1, 2, 3)
                .completes()
        }
    }

    class DelaySubscription {

        @Test
        fun `delaySubscription waits for trigger then emits source`() {
            Verify.that(Many.items(1, 2, 3).delaySubscription(Many.items(Unit)))
                .emitsNext(1, 2, 3)
                .completes()
        }

        @Test
        fun `delaySubscription propagates trigger error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.items(1, 2).delaySubscription(Many.error<Unit>(cause)))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `One delaySubscription emits value after delay`() {
            Verify.that(One.single(42).delaySubscription(10.milliseconds))
                .emitsNext(42)
                .completes()
        }

        @Test
        fun `One delaySubscription with trigger emits value after trigger`() {
            Verify.that(One.single(7).delaySubscription(Many.items(Unit)))
                .emitsNext(7)
                .completes()
        }

        @Test
        fun `Maybe delaySubscription emits present value after delay`() {
            Verify.that(Maybe.present(99).delaySubscription(10.milliseconds))
                .assertNext { assertEquals(99, it) }
                .completes()
        }

        @Test
        fun `Maybe delaySubscription on empty completes empty after delay`() {
            Verify.that(Maybe.empty<Int>().delaySubscription(10.milliseconds))
                .emitsCount(0).completes()
        }

        @Test
        fun `None delaySubscription completes after delay`() {
            Verify.that(None.complete<Int>().delaySubscription(10.milliseconds))
                .completes()
        }
    }

    class OnBackpressureDrop {

        @Test
        fun `onBackpressureDrop completes normally on small source`() {
            Verify.that(Many.items(1, 2, 3).onBackpressureDrop())
                .emitsNext(1, 2, 3)
                .completes()
        }

        @Test
        fun `onBackpressureDrop propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).onBackpressureDrop())

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }
    }

    class DistinctUntilChangedBy {

        @Test
        fun `distinctUntilChangedBy uses key for comparison`() {
            data class Item(val key: Int, val value: String)
            val items = listOf(Item(1, "a"), Item(1, "b"), Item(2, "c"), Item(2, "d"), Item(1, "e"))
            Verify.that(Many.from(items).distinctUntilChangedBy { it.key })
                .emitsNext(Item(1, "a"), Item(2, "c"), Item(1, "e"))
                .completes()
        }
    }

    class DoOnSubscribe {

        @Test
        fun `doOnSubscribe fires before any items`() {
            val fired = mutableListOf<String>()
            Verify.that(Many.items(1, 2, 3)
                .doOnSubscribe { fired.add("subscribed") }
                .doOnNext { fired.add("item") })
                .emitsCount(3)
                .completes()
            assertEquals("subscribed", fired.first())
        }
    }

    class DoFinally {

        @Test
        fun `doFinally fires on normal completion`() = runTest {
            var terminal: Signal.Terminal? = null
            Verify.that(Many.items(1, 2).doFinally { terminal = it })
                .emitsCount(2)
                .completes()
            assertIs<Signal.Upstream.Complete>(terminal)
        }

        @Test
        fun `doFinally fires on error`() = runTest {
            val cause = InvalidDemandException(-1)
            var terminal: Signal.Terminal? = null
            Verify.that(Many.error<Int>(cause).doFinally { terminal = it })

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
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
        fun `bufferTimeout emits bucket when size reached`() {
            val source = Many.items(1, 2, 3, 4)
            Verify.that(source.bufferTimeout(2, 5.seconds))
                .emitsNext(listOf(1, 2), listOf(3, 4))
                .completes()
        }

        @Test
        fun `bufferTimeout flushes partial bucket on source complete`() {
            Verify.that(Many.items(1, 2, 3).bufferTimeout(10, 5.seconds))
                .emitsNext(listOf(1, 2, 3))
                .completes()
        }

        @Test
        fun `bufferTimeout flushes on timeout`() {
            Verify.that(Many.items(1).bufferTimeout(100, 50.milliseconds))
                .emitsNext(listOf(1))
                .completes()
        }
    }

    class CombineLatest {

        @Test
        fun `combineLatest pairs latest values`() {
            val numbers = Many.items(1, 2)
            val letters = Many.items("a", "b")
            val result  = combineLatest(numbers, letters) { n, s -> "$n$s" }.toList().map { it.last() }
            Verify.that(result).emitsNext("2b").completes()
        }

        @Test
        fun `combineLatest on empty source completes without items`() {
            val empty   = Many.empty<Int>()
            val letters = Many.items("a")
            Verify.that(combineLatest(empty, letters) { n, s -> "$n$s" })
                .completes()
        }
    }

    class ZipOne {

        @Test
        fun `zip pairs two One values`() {
            val one   = One.single(1)
            val other = One.single("a")
            Verify.that(zip(one, other) { n, s -> "$n$s" })
                .emitsNext("1a")
                .completes()
        }

        @Test
        fun `zip completes empty when first source is empty`() = runTest {
            val empty  = One.defer<Int> { throw NoElementException() }.recover { One.single(0) }
                .flatMap { One.generate<Int> { emit -> emit(Signal.Upstream.Complete) } }
            val other  = One.single("a")
            val result = zip(empty, other) { n, s -> "$n$s" }.await()
            assertIs<Failure<AelvException>>(result)
        }

        @Test
        fun `zip propagates error from first source`() {
            val cause  = InvalidDemandException(-1)
            val first  = One.error<Int>(cause)
            val second = One.single("a")
            Verify.that(zip(first, second) { n, s -> "$n$s" })
                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `zip propagates error from second source`() {
            val cause = InvalidDemandException(-1)
            Verify.that(zip(One.single(1), One.error<String>(cause)) { n, s -> "$n$s" })

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `zipWith is alias for zip`() {
            Verify.that(One.single(2).zipWith(One.single(3)) { a, b -> a + b })
                .emitsNext(5)
                .completes()
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
            Verify.that(One.error<Int>(cause).flatMapNone { None.complete<Int>() })

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
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
                .completes()
        }

        @Test
        fun `subscribeOn does not lose items`() {
            Verify.that(Many.items(1, 2, 3).subscribeOn(Dispatchers.Default))
                .emitsNext(1, 2, 3)
                .completes()
        }
    }

    class Reduce {

        @Test
        fun `reduce accumulates all items`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).reduce { a, b -> a + b })
                .emitsNext(15)
                .completes()
        }

        @Test
        fun `reduce on empty returns NoElementException`() {
            Verify.that(Many.empty<Int>().reduce { a, b -> a + b })
                .failsWith<NoElementException>()
        }

        @Test
        fun `reduce propagates source error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Many.error<Int>(cause).reduce { a, b -> a + b })

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }
    }

    class Interval {

        @Test
        fun `interval emits incrementing longs starting at zero`() {
            Verify.that(Many.interval(10.milliseconds).take(3))
                .emitsNext(0L, 1L, 2L)
                .completes()
        }
    }

    class BufferSkip {

        @Test
        fun `buffer with skip equal to size is same as buffer by size`() {
            Verify.that(Many.items(1, 2, 3, 4).buffer(2, 2))
                .emitsNext(listOf(1, 2), listOf(3, 4))
                .completes()
        }

        @Test
        fun `buffer with skip greater than size creates gaps`() {
            Verify.that(Many.items(1, 2, 3, 4, 5).buffer(2, 3))
                .emitsNext(listOf(1, 2), listOf(4, 5))
                .completes()
        }

        @Test
        fun `buffer with skip less than size creates overlapping windows`() = runTest {
            val result = Many.items(1, 2, 3, 4).buffer(3, 1).toList().await()
            assertIs<Success<List<List<Int>>>>(result)
            assertTrue(result.value.contains(listOf(1, 2, 3)))
            assertTrue(result.value.contains(listOf(2, 3, 4)))
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
                .completes()
        }

        @Test
        fun `Maybe retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(Maybe.error<Int>(cause).retry(times = 2))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }

        @Test
        fun `Maybe retry signals onComplete to direct subscriber after successful retry`() = runTest {
            var retries = 0
            var completed = false
            val signalsCompleteAfterRetries = Maybe.defer { if (retries < 1) throw InvalidDemandException(-1) }
                .doOnRetry { _,_ -> retries++ }.retry(times = 1)
                .doOnComplete { if(!completed) completed = true else throw IllegalStateException("only once") }
            Verify.that(signalsCompleteAfterRetries).completes()
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
                .completes()
            assertEquals(3, attempts)
        }

        @Test
        fun `None retry exhausts and propagates error`() {
            val cause = InvalidDemandException(-1)
            Verify.that(None.error<Unit>(cause).retry(times = 2))

                .failsWith<InvalidDemandException> { assertEquals(cause, it) }
        }
    }

    class CancelSemantics {

        @Test
        fun `aborted pipeline does not deliver items`() {
            Verify.that(Many.items(1, 2, 3).take(0)).cancels()
        }

        @Test
        fun `completed pipeline delivers all items`() {
            Verify.that(Many.items(1, 2, 3))
                .emitsNext(1, 2, 3)
                .completes()
        }
    }

    class TimeoutTest {

        @Test
        fun `Many timeout fires when stream does not complete in time`() {
            Verify.that(Many.never<Int>().timeout(50.milliseconds))
                .failsWith<ExceededTimeoutException>()
        }

        @Test
        fun `Many timeout passes through when stream completes in time`() {
            Verify.that(Many.items(1, 2, 3).timeout(5.seconds))
                .emitsNext(1, 2, 3)
                .completes()
        }

        @Test
        fun `One timeout fires when value does not arrive in time`() {
            Verify.that(One.never<Int>().timeout(50.milliseconds))
                .failsWith<ExceededTimeoutException>()
        }

        @Test
        fun `One timeout passes through when value arrives in time`() {
            Verify.that(One.single(42).timeout(5.seconds))
                .assertNext { assertEquals(42, it) }
                .completes()
        }

        @Test
        fun `Maybe timeout fires when neither value nor empty arrives in time`() {
            Verify.that(Maybe.never<Int>().timeout(50.milliseconds))
                .failsWith<ExceededTimeoutException>()
        }

        @Test
        fun `Maybe timeout passes through present value in time`() {
            Verify.that(Maybe.present(7).timeout(5.seconds))
                .assertNext { assertEquals(7, it) }
                .completes()
        }

        @Test
        fun `Maybe timeout passes through empty completion in time`() {
            Verify.that(Maybe.empty<Int>().timeout(5.seconds))
                .emitsCount(0).completes()
        }

        @Test
        fun `timeout does not include downstream processing time`() {
            val slow: suspend (Int) -> Many<Int> = { v -> delay(200.milliseconds); Many.items(v) }
            Verify.that(Many.items(1, 2, 3)
                .timeout(100.milliseconds)
                .concatMap(transform = slow))
                .emitsNext(1, 2, 3)
                .completes()
        }
    }

    class Scan {

        @Test
        fun `scan accumulates running total`() {
            Verify.that(Many.items(1, 2, 3, 4).scan(0) { acc, item -> acc + item })
                .emitsNext(1, 3, 6, 10)
                .completes()
        }

        @Test
        fun `scan on empty source emits nothing`() {
            Verify.that(Many.empty<Int>().scan(0) { acc, item -> acc + item })
                .completes()
        }

        @Test
        fun `scan propagates error`() {
            Verify.that(Many.items(1, 2).scan(0) { _, _ -> throw InvalidDemandException(-1) })
                .failsWith<InvalidDemandException>()
        }
    }

    class FlatMapOne {

        @Test
        fun `flatMapOne maps each item through a One`() {
            Verify.that(Many.items(1, 2, 3).flatMapOne { One.single(it * 10) })
                .emitsNext(10, 20, 30)
                .completes()
        }

        @Test
        fun `flatMapOne on empty source emits nothing`() {
            Verify.that(Many.empty<Int>().flatMapOne { One.single(it) })
                .completes()
        }

        @Test
        fun `flatMapOne propagates error from source`() {
            Verify.that(Many.error<Int>(InvalidDemandException(-1)).flatMapOne { One.single(it) })
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `flatMapOne propagates error from inner One`() {
            Verify.that(Many.items(1).flatMapOne { One.error<Int>(InvalidDemandException(-1)) })
                .failsWith<InvalidDemandException>()
        }
    }
}
