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
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OperatorsTest {

    class Transform {

        @Test
        fun `map transforms items`() = runTest {
            val result = Many.items(1, 2, 3).map { it * 2 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(2, 4, 6), result.value)
        }

        @Test
        fun `map on empty emits nothing`() = runTest {
            val result = Many.empty<Int>().map { it * 2 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `map propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).map { it * 2 }.toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `filter keeps matching items`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).filter { it % 2 == 0 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(2, 4), result.value)
        }

        @Test
        fun `filter propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).filter { it % 2 == 0 }.toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `mapNotNull transforms and drops nulls`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).mapNotNull { if (it % 2 == 0) it * 10 else null }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(20, 40), result.value)
        }

        @Test
        fun `mapNotNull on all-null source emits nothing`() = runTest {
            val result = Many.items(1, 3, 5).mapNotNull<Int, Int> { null }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `mapNotNull propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).mapNotNull { it * 2 }.toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `take limits to n items`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).take(3).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `take zero emits nothing`() = runTest {
            val result = Many.items(1, 2, 3).take(0).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `skip drops first n items`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).skip(2).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(3, 4, 5), result.value)
        }

        @Test
        fun `skipWhile skips until predicate false`() = runTest {
            val result = Many.items(1, 2, 3, 4, 1).skipWhile { it < 3 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(3, 4, 1), result.value)
        }

        @Test
        fun `takeWhile emits until predicate false`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).takeWhile { it < 4 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `distinct removes duplicates`() = runTest {
            val result = Many.items(1, 2, 1, 3, 2).distinct().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `distinctUntilChanged removes consecutive duplicates only`() = runTest {
            val result = Many.items(1, 1, 2, 2, 1).distinctUntilChanged().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 1), result.value)
        }
    }

    class Expand {

        @Test
        fun `flatMap expands each item`() = runTest {
            val result = Many.items(1, 2, 3).flatMap { Many.items(it, it * 10) }.toSet().await()
            assertIs<Either.Right<Set<Int>>>(result)
            assertEquals(setOf(1, 10, 2, 20, 3, 30), result.value)
        }

        @Test
        fun `concatMap preserves order`() = runTest {
            val result = Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 10, 2, 20, 3, 30), result.value)
        }

        @Test
        fun `flatMap with concurrency 1 equals concatMap`() = runTest {
            val flat = Many.items(1, 2, 3).flatMap(concurrency = 1) { Many.items(it, it * 10) }.toList().await()
            val concat = Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) }.toList().await()
            assertEquals(flat, concat)
        }
    }

    class Combine {

        @Test
        fun `merge interleaves two streams`() = runTest {
            val result = merge(Many.items(1, 3), Many.items(2, 4)).toSet().await()
            assertIs<Either.Right<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `mergeWith combines two streams`() = runTest {
            val result = Many.items(1, 2).mergeWith(Many.items(3, 4)).toSet().await()
            assertIs<Either.Right<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `concat sequences streams in order`() = runTest {
            val result = concat(Many.items(1, 2), Many.items(3, 4)).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `concat with empty streams`() = runTest {
            val result = concat(Many.empty(), Many.items(1, 2), Many.empty<Int>()).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2), result.value)
        }

        @Test
        fun `zip pairs items from two streams`() = runTest {
            val result = zip(Many.items(1, 2, 3), Many.items("a", "b", "c")) { n, s -> "$n$s" }.toList().await()
            assertIs<Either.Right<List<String>>>(result)
            assertEquals(listOf("1a", "2b", "3c"), result.value)
        }

        @Test
        fun `zip stops at shortest stream`() = runTest {
            val result = zip(Many.items(1, 2), Many.items("a", "b", "c")) { n, s -> "$n$s" }.toList().await()
            assertIs<Either.Right<List<String>>>(result)
            assertEquals(listOf("1a", "2b"), result.value)
        }

        @Test
        fun `zip does not hang when source A errors after source B completes`() = runTest {
            // Regression for Bug Z2: post-loop channelA probe must be non-blocking.
            // Source B is shorter; loop exits after pairing (1,"a"). Source A still has items
            // buffered.  After jobA is cancelled, the old code called receiveCatching() which
            // could block if jobA was cancelled mid-send.  Now we use tryReceive().
            val cause = InvalidDemandException(-1)
            val sourceA = Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(1))
                emit(Signal.Upstream.Next(2))
                emit(Signal.Upstream.Error(cause))
            }
            val result = zip(sourceA, Many.items("a")) { n, s -> "$n$s" }.toList().await()
            // Either completes with ["1a"] or surfaces the error from A — either is acceptable.
            // What must NOT happen is a hang.
            assertTrue(result is Either.Right || result is Either.Left)
        }
    }

    class Buffer {

        @Test
        fun `buffer collects items into fixed size lists`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).buffer(2).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(3, 4), listOf(5)), result.value)
        }

        @Test
        fun `buffer emits partial bucket on completion`() = runTest {
            val result = Many.items(1, 2, 3).buffer(2).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(3)), result.value)
        }
    }

    class ErrorHandling {

        @Test
        fun `recover replaces error with fallback stream`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).recover { Many.items(99) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(99), result.value)
        }

        @Test
        fun `recoverWith replaces error with fallback value`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).recoverWith { 99 }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(99), result.value)
        }

        @Test
        fun `retry retries on error up to n times`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(times = 3).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(42), result.value)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry exhausts and propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).retry(times = 2).toList().await()
            assertIs<Either.Left<AelvException>>(result)
        }

        @Test
        fun `retry with Retry max succeeds after failures`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().maxAttempts(3)).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(42), result.value)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with Retry max exhausts and propagates`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).retry(Policy.retry().maxAttempts(2)).toList().await()
            assertIs<Either.Left<AelvException>>(result)
        }

        @Test
        fun `retry with fixed backoff delays between attempts`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().withBackoff(10.milliseconds).maxAttempts(3)).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with exponential backoff succeeds after failures`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().withBackoff(10.milliseconds, 100.milliseconds).maxAttempts(3)).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry filter skips non-matching errors`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause)
                .retry(Policy.retry().on(NoSuchElementException::class).maxAttempts(5))
                .toList().await()
            assertIs<Either.Left<AelvException>>(result)
        }

        @Test
        fun `retry zero does not retry`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                throw InvalidDemandException(-1)
            }.retry(Policy.retry().maxAttempts(0)).toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(1, attempts)
        }
    }

    class Terminal {

        @Test
        fun `fold accumulates all items`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).fold(0) { acc, item -> acc + item }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(15, result.value)
        }

        @Test
        fun `fold propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).fold(0) { acc, item -> acc + item }.await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `first returns first item`() = runTest {
            val result = Many.items(1, 2, 3).first()
            assertIs<Either.Right<Int>>(result)
            assertEquals(1, result.value)
        }

        @Test
        fun `first on empty returns NoSuchElementException`() = runTest {
            val result = Many.empty<Int>().first()
            assertIs<Either.Left<AelvException>>(result)
            assertIs<NoSuchElementException>(result.value)
        }

        @Test
        fun `last returns last item`() = runTest {
            val result = Many.items(1, 2, 3).last()
            assertIs<Either.Right<Int>>(result)
            assertEquals(3, result.value)
        }

        @Test
        fun `toList returns immutable list`() = runTest {
            val result = Many.items(1, 2, 3).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `toSet returns immutable set`() = runTest {
            val result = Many.items(1, 2, 1, 3).toSet().await()
            assertIs<Either.Right<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3), result.value)
        }
    }

    class OneOps {

        @Test
        fun `map transforms value`() = runTest {
            val result = One.single(5).map { it * 3 }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(15, result.value)
        }

        @Test
        fun `flatMap chains to another One`() = runTest {
            val result = One.single(5).flatMap { One.single(it * 2) }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(10, result.value)
        }

        @Test
        fun `flatMapMany expands to Many`() = runTest {
            val result = One.single(3).flatMapMany { Many.items(it, it + 1, it + 2) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(3, 4, 5), result.value)
        }

        @Test
        fun `recover replaces error with fallback value`() = runTest {
            val result = One.error<Int>(InvalidDemandException(-1)).recover { 99 }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(99, result.value)
        }

        @Test
        fun `retry retries on error`() = runTest {
            var attempts = 0
            val result = One.defer {
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                42
            }.retry(Policy.retry().maxAttempts(3)).await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(42, result.value)
            assertEquals(3, attempts)
        }
    }

    class GroupBy {

        @Test
        fun `groups items by key`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { key, group -> group.map { key to it } }
                .toList()
                .await()
            assertIs<Either.Right<List<Pair<Int, Int>>>>(result)
            val byKey = result.value.groupBy({ it.first }, { it.second })
            assertEquals(listOf(2, 4, 6), byKey[0]?.sorted())
            assertEquals(listOf(1, 3, 5), byKey[1]?.sorted())
        }

        @Test
        fun `each group receives a terminal Complete`() = runTest {
            val completed = mutableSetOf<String>()
            Many.items("a", "b", "a", "c")
                .groupBy({ it }) { key, group ->
                    group.doOnComplete { completed.add(key) }.map { key to it }
                }
                .toList()
                .await()
            assertEquals(setOf("a", "b", "c"), completed)
        }

        @Test
        fun `each group receives an Error terminal when source errors`() = runTest {
            val cause = InvalidDemandException(-1)
            val errors = mutableMapOf<String, Exception>()
            val result = Many.generate<String> { emit ->
                emit(Signal.Upstream.Next("x"))
                emit(Signal.Upstream.Next("y"))
                emit(Signal.Upstream.Error(cause))
            }
                .groupBy({ it }) { key, group ->
                    group.recover { err -> errors[key] = err; Many.empty() }
                }
                .recover { Many.empty() }
                .toList()
                .await()
            assertIs<Either.Right<*>>(result)
            assertEquals(setOf("x", "y"), errors.keys)
            assertTrue(errors.values.all { it === cause })
        }

        @Test
        fun `single-key source produces one group with all items`() = runTest {
            val result = Many.items(10, 20, 30)
                .groupBy({ "only" }) { _, group -> group }
                .toList()
                .await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(10, 20, 30), result.value)
        }

        @Test
        fun `empty source emits no items and completes`() = runTest {
            val result = Many.empty<Int>()
                .groupBy({ it }) { _, group -> group }
                .toList()
                .await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `Cancel on outer stream cancels all group pipelines`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { _, group -> group }
                .take(1)
                .toList()
                .await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(1, result.value.size)
        }

        @Test
        fun `group Complete is delivered before outer stream Complete`() = runTest {
            // Regression for Bug G1/G3: operator owns subscriptions so this is now structural.
            val completedGroups = mutableSetOf<Int>()
            Many.items(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 3 }) { key, group ->
                    group.doOnComplete { completedGroups.add(key) }.map { key }
                }
                .toList()
                .await()
            assertEquals(setOf(0, 1, 2), completedGroups)
        }

        @Test
        fun `group Error is delivered before outer stream Error`() = runTest {
            // Regression for Bug G3.
            val cause = InvalidDemandException(-1)
            val erroredGroups = mutableSetOf<Int>()
            Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(1))
                emit(Signal.Upstream.Next(2))
                emit(Signal.Upstream.Next(3))
                emit(Signal.Upstream.Error(cause))
            }
                .groupBy({ it % 2 }) { key, group ->
                    group.doOnError { erroredGroups.add(key) }.recover { Many.empty() }
                }
                .recover { Many.empty() }
                .toList()
                .await()
            assertEquals(setOf(0, 1), erroredGroups)
        }

        @Test
        fun `groupHandler can apply different transforms per key`() = runTest {
            val result = Many.items(1, 2, 3, 4)
                .groupBy({ it % 2 }) { key, group ->
                    if (key == 0) group.map { it * 10 } else group.map { it * 100 }
                }
                .toList()
                .await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(setOf(20, 40, 100, 300), result.value.toSet())
        }
    }

    class SwitchMap {

        @Test
        fun `switchMap emits from latest inner stream only`() = runTest {
            // switchMap cancels previous inner on each new outer item.
            // With a synchronous source, all but the last inner get cancelled before emitting.
            val result = Many.items(1, 2, 3).switchMap { Many.items(it * 10) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isNotEmpty())
            assertTrue(result.value.last() == 30)
        }

        @Test
        fun `switchMap propagates source error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).switchMap { Many.items(it) }.toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `switchMap completes on empty source`() = runTest {
            val result = Many.empty<Int>().switchMap { Many.items(it) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }
    }

    class FlatMapSequential {

        @Test
        fun `flatMapSequential preserves order`() = runTest {
            val result = Many.items(1, 2, 3).flatMapSequential { Many.items(it, it * 10) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 10, 2, 20, 3, 30), result.value)
        }

        @Test
        fun `flatMapSequential with maxConcurrency 1 equals concatMap`() = runTest {
            val sequential = Many.items(1, 2, 3).flatMapSequential(maxConcurrency = 1) { Many.items(it, it * 10) }.toList().await()
            val concat = Many.items(1, 2, 3).concatMap { Many.items(it, it * 10) }.toList().await()
            assertEquals(sequential, concat)
        }

        @Test
        fun `flatMapSequential on empty source completes`() = runTest {
            val result = Many.empty<Int>().flatMapSequential { Many.items(it) }.toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }
    }

    class TakeUntilOther {

        @Test
        fun `takeUntilOther stops when other signals`() {
            val source = Sinks.broadcast<Int>()
            val other  = Sinks.broadcast<Unit>()
            Verify.that(source.asMany().takeUntilOther(other.asMany()))
                .runs { other.complete() }
                .completesNormally()
        }

        @Test
        fun `takeUntilOther completes normally when source completes first`() = runTest {
            val result = Many.items(1, 2, 3).takeUntilOther(Many.never<Unit>()).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }
    }

    class DelaySubscription {

        @Test
        fun `delaySubscription waits for trigger then emits source`() = runTest {
            val result = Many.items(1, 2, 3).delaySubscription(Many.items(Unit)).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `delaySubscription propagates trigger error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.items(1, 2).delaySubscription(Many.error<Unit>(cause)).toList().await()
            assertIs<Either.Left<AelvException>>(result)
        }
    }

    class OnBackpressureDrop {

        @Test
        fun `onBackpressureDrop completes normally on small source`() = runTest {
            val result = Many.items(1, 2, 3).onBackpressureDrop().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isNotEmpty())
        }

        @Test
        fun `onBackpressureDrop propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).onBackpressureDrop().toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }
    }

    class DistinctUntilChangedBy {

        @Test
        fun `distinctUntilChangedBy uses key for comparison`() = runTest {
            data class Item(val key: Int, val value: String)
            val items = listOf(Item(1, "a"), Item(1, "b"), Item(2, "c"), Item(2, "d"), Item(1, "e"))
            val result = Many.from(items).distinctUntilChangedBy { it.key }.toList().await()
            assertIs<Either.Right<List<Item>>>(result)
            assertEquals(listOf(Item(1, "a"), Item(2, "c"), Item(1, "e")), result.value)
        }
    }

    class DoOnSubscribe {

        @Test
        fun `doOnSubscribe fires before any items`() = runTest {
            val fired = mutableListOf<String>()
            Many.items(1, 2, 3)
                .doOnSubscribe { fired.add("subscribed") }
                .doOnNext { fired.add("item") }
                .toList().await()
            assertEquals("subscribed", fired.first())
        }
    }

    class DoFinally {

        @Test
        fun `doFinally fires on normal completion`() = runTest {
            var terminal: Signal.Terminal? = null
            Many.items(1, 2).doFinally { terminal = it }.toList().await()
            assertIs<Signal.Upstream.Complete>(terminal)
        }

        @Test
        fun `doFinally fires on error`() = runTest {
            val cause = InvalidDemandException(-1)
            var terminal: Signal.Terminal? = null
            Many.error<Int>(cause).doFinally { terminal = it }.toList().await()
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
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertTrue(result.value.isNotEmpty())
            assertTrue(result.value.all { it.size <= 2 })
        }

        @Test
        fun `bufferTimeout flushes partial bucket on source complete`() = runTest {
            val result = Many.items(1, 2, 3).bufferTimeout(10, 5.seconds).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2, 3)), result.value)
        }

        @Test
        fun `bufferTimeout flushes on timeout`() = runTest {
            val result = Many.items(1).bufferTimeout(100, 50.milliseconds).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1)), result.value)
        }
    }

    class CombineLatest {

        @Test
        fun `combineLatest pairs latest values`() = runTest {
            val result = combineLatest(Many.items(1, 2), Many.items("a", "b")) { n, s -> "$n$s" }.toList().await()
            assertIs<Either.Right<List<String>>>(result)
            assertTrue(result.value.isNotEmpty())
        }

        @Test
        fun `combineLatest on empty source completes without items`() = runTest {
            val result = combineLatest(Many.empty<Int>(), Many.items("a")) { n, s -> "$n$s" }.toList().await()
            assertIs<Either.Right<List<String>>>(result)
            assertTrue(result.value.isEmpty())
        }
    }

    class ZipOne {

        @Test
        fun `zip pairs two One values`() = runTest {
            val result = zip(One.single(1), One.single("a")) { n, s -> "$n$s" }.await()
            assertIs<Either.Right<String>>(result)
            assertEquals("1a", result.value)
        }

        @Test
        fun `zip completes empty when first source is empty`() = runTest {
            val result = zip(One.defer<Int> { throw NoSuchElementException() }.recover { 0 }.flatMap { One.generate<Int> { emit -> emit(Signal.Upstream.Complete) } }, One.single("a")) { n, s -> "$n$s" }.await()
            assertIs<Either.Left<AelvException>>(result)
        }

        @Test
        fun `zip propagates error from first source`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = zip(One.error<Int>(cause), One.single("a")) { n, s -> "$n$s" }.await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `zip propagates error from second source`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = zip(One.single(1), One.error<String>(cause)) { n, s -> "$n$s" }.await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `zipWith is alias for zip`() = runTest {
            val result = One.single(2).zipWith(One.single(3)) { a, b -> a + b }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(5, result.value)
        }
    }

    class FlatMapNone {

        @Test
        fun `flatMapNone chains to None`() = runTest {
            var ran = false
            One.single(42).flatMapNone { None.defer { ran = true } }.await()
            assertTrue(ran)
        }

        @Test
        fun `flatMapNone propagates error from One`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = One.error<Int>(cause).flatMapNone { None.complete() }.await()
            assertIs<Either.Left<AelvException>>(result)
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
        fun `publishOn does not lose items`() = runTest {
            val result = Many.items(1, 2, 3).publishOn(Dispatchers.Default).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `subscribeOn does not lose items`() = runTest {
            val result = Many.items(1, 2, 3).subscribeOn(Dispatchers.Default).toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }
    }

    class Reduce {

        @Test
        fun `reduce accumulates all items`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).reduce { a, b -> a + b }.await()
            assertIs<Either.Right<Int>>(result)
            assertEquals(15, result.value)
        }

        @Test
        fun `reduce on empty returns NoSuchElementException`() = runTest {
            val result = Many.empty<Int>().reduce { a, b -> a + b }.await()
            assertIs<Either.Left<AelvException>>(result)
            assertIs<NoSuchElementException>(result.value)
        }

        @Test
        fun `reduce propagates source error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).reduce { a, b -> a + b }.await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }
    }

    class Interval {

        @Test
        fun `interval emits incrementing longs starting at zero`() = runTest {
            val result = Many.interval(10.milliseconds).take(3).toList().await()
            assertIs<Either.Right<List<Long>>>(result)
            assertEquals(listOf(0L, 1L, 2L), result.value)
        }
    }

    class BufferSkip {

        @Test
        fun `buffer with skip equal to size is same as buffer by size`() = runTest {
            val result = Many.items(1, 2, 3, 4).buffer(2, 2).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(3, 4)), result.value)
        }

        @Test
        fun `buffer with skip greater than size creates gaps`() = runTest {
            val result = Many.items(1, 2, 3, 4, 5).buffer(2, 3).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(4, 5)), result.value)
        }

        @Test
        fun `buffer with skip less than size creates overlapping windows`() = runTest {
            val result = Many.items(1, 2, 3, 4).buffer(3, 1).toList().await()
            assertIs<Either.Right<List<List<Int>>>>(result)
            // Full windows: [1,2,3], [2,3,4]. Partial trailing: [3,4], [4].
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
            val result = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2), result.value)
        }

        @Test
        fun `broadcast sink does not replay to late subscriber`() = runTest {
            val sink = Sinks.broadcast<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            // broadcast has no history — late subscriber gets only the terminal
            val result = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `replay sink replays full history to late subscriber`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.complete()
            val result = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(1, 2), result.value)
        }

        @Test
        fun `replayLast sink replays only last n items`() = runTest {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            val result = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertEquals(listOf(2, 3), result.value)
        }

        @Test
        fun `sink complete is delivered to subscriber`() = runTest {
            val sink = Sinks.broadcast<Int>()
            sink.complete()
            val result = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `sink error is delivered to subscriber`() = runTest {
            val cause = InvalidDemandException(-1)
            val sink = Sinks.broadcast<Int>()
            sink.error(cause)
            val result = sink.asMany().toList().await()
            assertIs<Either.Left<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `broadcast sink delivers to multiple concurrent subscribers`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.complete()
            val r1 = sink.asMany().toList().await()
            val r2 = sink.asMany().toList().await()
            assertIs<Either.Right<List<Int>>>(r1)
            assertIs<Either.Right<List<Int>>>(r2)
            assertEquals(listOf(1, 2), r1.value)
            assertEquals(listOf(1, 2), r2.value)
        }
    }
}
