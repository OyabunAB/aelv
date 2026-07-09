package se.oyabun.aelv

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class OperatorsTest {

    class Transform {

        @Test
        fun `map transforms items`() = runTest {
            val result = Many.of(1, 2, 3).map { it * 2 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(2, 4, 6), result.value)
        }

        @Test
        fun `map on empty emits nothing`() = runTest {
            val result = Many.empty<Int>().map { it * 2 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `map propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).map { it * 2 }.toList().get()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `filter keeps matching items`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).filter { it % 2 == 0 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(2, 4), result.value)
        }

        @Test
        fun `take limits to n items`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).take(3).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `take zero emits nothing`() = runTest {
            val result = Many.of(1, 2, 3).take(0).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `skip drops first n items`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).skip(2).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(3, 4, 5), result.value)
        }

        @Test
        fun `skipWhile skips until predicate false`() = runTest {
            val result = Many.of(1, 2, 3, 4, 1).skipWhile { it < 3 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(3, 4, 1), result.value)
        }

        @Test
        fun `takeWhile emits until predicate false`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).takeWhile { it < 4 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `distinct removes duplicates`() = runTest {
            val result = Many.of(1, 2, 1, 3, 2).distinct().toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `distinctUntilChanged removes consecutive duplicates only`() = runTest {
            val result = Many.of(1, 1, 2, 2, 1).distinctUntilChanged().toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 1), result.value)
        }
    }

    class Expand {

        @Test
        fun `flatMap expands each item`() = runTest {
            val result = Many.of(1, 2, 3).flatMap { Many.of(it, it * 10) }.toSet().get()
            assertIs<Either.Left<Set<Int>>>(result)
            assertEquals(setOf(1, 10, 2, 20, 3, 30), result.value)
        }

        @Test
        fun `concatMap preserves order`() = runTest {
            val result = Many.of(1, 2, 3).concatMap { Many.of(it, it * 10) }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 10, 2, 20, 3, 30), result.value)
        }

        @Test
        fun `flatMap with concurrency 1 equals concatMap`() = runTest {
            val flat = Many.of(1, 2, 3).flatMap(concurrency = 1) { Many.of(it, it * 10) }.toList().get()
            val concat = Many.of(1, 2, 3).concatMap { Many.of(it, it * 10) }.toList().get()
            assertEquals(flat, concat)
        }
    }

    class Combine {

        @Test
        fun `merge interleaves two streams`() = runTest {
            val result = merge(Many.of(1, 3), Many.of(2, 4)).toSet().get()
            assertIs<Either.Left<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `mergeWith combines two streams`() = runTest {
            val result = Many.of(1, 2).mergeWith(Many.of(3, 4)).toSet().get()
            assertIs<Either.Left<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `concat sequences streams in order`() = runTest {
            val result = concat(Many.of(1, 2), Many.of(3, 4)).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3, 4), result.value)
        }

        @Test
        fun `concat with empty streams`() = runTest {
            val result = concat(Many.empty(), Many.of(1, 2), Many.empty<Int>()).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2), result.value)
        }

        @Test
        fun `zip pairs items from two streams`() = runTest {
            val result = zip(Many.of(1, 2, 3), Many.of("a", "b", "c")) { n, s -> "$n$s" }.toList().get()
            assertIs<Either.Left<List<String>>>(result)
            assertEquals(listOf("1a", "2b", "3c"), result.value)
        }

        @Test
        fun `zip stops at shortest stream`() = runTest {
            val result = zip(Many.of(1, 2), Many.of("a", "b", "c")) { n, s -> "$n$s" }.toList().get()
            assertIs<Either.Left<List<String>>>(result)
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
            val result = zip(sourceA, Many.of("a")) { n, s -> "$n$s" }.toList().get()
            // Either completes with ["1a"] or surfaces the error from A — either is acceptable.
            // What must NOT happen is a hang.
            assertTrue(result is Either.Left || result is Either.Right)
        }
    }

    class Buffer {

        @Test
        fun `buffer collects items into fixed size lists`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).buffer(2).toList().get()
            assertIs<Either.Left<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(3, 4), listOf(5)), result.value)
        }

        @Test
        fun `buffer emits partial bucket on completion`() = runTest {
            val result = Many.of(1, 2, 3).buffer(2).toList().get()
            assertIs<Either.Left<List<List<Int>>>>(result)
            assertEquals(listOf(listOf(1, 2), listOf(3)), result.value)
        }
    }

    class ErrorHandling {

        @Test
        fun `recover replaces error with fallback stream`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).recover { Many.of(99) }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(99), result.value)
        }

        @Test
        fun `recoverWith replaces error with fallback value`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).recoverWith { 99 }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
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
            }.retry(times = 3).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(42), result.value)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry exhausts and propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).retry(times = 2).toList().get()
            assertIs<Either.Right<AelvException>>(result)
        }

        @Test
        fun `retry with Retry max succeeds after failures`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().maxAttempts(3)).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(42), result.value)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry with Retry max exhausts and propagates`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).retry(Policy.retry().maxAttempts(2)).toList().get()
            assertIs<Either.Right<AelvException>>(result)
        }

        @Test
        fun `retry with fixed backoff delays between attempts`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                emit(Signal.Upstream.Next(42))
                emit(Signal.Upstream.Complete)
            }.retry(Policy.retry().withBackoff(10.milliseconds).maxAttempts(3)).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
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
            }.retry(Policy.retry().withBackoff(10.milliseconds, 100.milliseconds).maxAttempts(3)).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(3, attempts)
        }

        @Test
        fun `retry filter skips non-matching errors`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause)
                .retry(Policy.retry().on(NoSuchElementException::class).maxAttempts(5))
                .toList().get()
            assertIs<Either.Right<AelvException>>(result)
        }

        @Test
        fun `retry zero does not retry`() = runTest {
            var attempts = 0
            val result = Many.generate<Int> { emit ->
                attempts++
                throw InvalidDemandException(-1)
            }.retry(Policy.retry().maxAttempts(0)).toList().get()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(1, attempts)
        }
    }

    class Terminal {

        @Test
        fun `fold accumulates all items`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5).fold(0) { acc, item -> acc + item }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(15, result.value)
        }

        @Test
        fun `fold propagates error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).fold(0) { acc, item -> acc + item }.get()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `first returns first item`() = runTest {
            val result = Many.of(1, 2, 3).first()
            assertIs<Either.Left<Int>>(result)
            assertEquals(1, result.value)
        }

        @Test
        fun `first on empty returns NoSuchElementException`() = runTest {
            val result = Many.empty<Int>().first()
            assertIs<Either.Right<AelvException>>(result)
            assertIs<NoSuchElementException>(result.value)
        }

        @Test
        fun `last returns last item`() = runTest {
            val result = Many.of(1, 2, 3).last()
            assertIs<Either.Left<Int>>(result)
            assertEquals(3, result.value)
        }

        @Test
        fun `toList returns immutable list`() = runTest {
            val result = Many.of(1, 2, 3).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `toSet returns immutable set`() = runTest {
            val result = Many.of(1, 2, 1, 3).toSet().get()
            assertIs<Either.Left<Set<Int>>>(result)
            assertEquals(setOf(1, 2, 3), result.value)
        }
    }

    class OneOps {

        @Test
        fun `map transforms value`() = runTest {
            val result = One.of(5).map { it * 3 }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(15, result.value)
        }

        @Test
        fun `flatMap chains to another One`() = runTest {
            val result = One.of(5).flatMap { One.of(it * 2) }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(10, result.value)
        }

        @Test
        fun `flatMapMany expands to Many`() = runTest {
            val result = One.of(3).flatMapMany { Many.of(it, it + 1, it + 2) }.toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(3, 4, 5), result.value)
        }

        @Test
        fun `recover replaces error with fallback value`() = runTest {
            val result = One.error<Int>(InvalidDemandException(-1)).recover { 99 }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(99, result.value)
        }

        @Test
        fun `retry retries on error`() = runTest {
            var attempts = 0
            val result = One.defer {
                attempts++
                if (attempts < 3) throw InvalidDemandException(-1)
                42
            }.retry(Policy.retry().maxAttempts(3)).get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(42, result.value)
            assertEquals(3, attempts)
        }
    }

    class GroupBy {

        @Test
        fun `groups items by key`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { key, group -> group.map { key to it } }
                .toList()
                .get()
            assertIs<Either.Left<List<Pair<Int, Int>>>>(result)
            val byKey = result.value.groupBy({ it.first }, { it.second })
            assertEquals(listOf(2, 4, 6), byKey[0]?.sorted())
            assertEquals(listOf(1, 3, 5), byKey[1]?.sorted())
        }

        @Test
        fun `each group receives a terminal Complete`() = runTest {
            val completed = mutableSetOf<String>()
            Many.of("a", "b", "a", "c")
                .groupBy({ it }) { key, group ->
                    group.doOnComplete { completed.add(key) }.map { key to it }
                }
                .toList()
                .get()
            assertEquals(setOf("a", "b", "c"), completed)
        }

        @Test
        fun `each group receives an Error terminal when source errors`() = runTest {
            val cause = InvalidDemandException(-1)
            val errors = mutableMapOf<String, AelvException>()
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
                .get()
            assertIs<Either.Left<*>>(result)
            assertEquals(setOf("x", "y"), errors.keys)
            assertTrue(errors.values.all { it === cause })
        }

        @Test
        fun `single-key source produces one group with all items`() = runTest {
            val result = Many.of(10, 20, 30)
                .groupBy({ "only" }) { _, group -> group }
                .toList()
                .get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(10, 20, 30), result.value)
        }

        @Test
        fun `empty source emits no items and completes`() = runTest {
            val result = Many.empty<Int>()
                .groupBy({ it }) { _, group -> group }
                .toList()
                .get()
            assertIs<Either.Left<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `Cancel on outer stream cancels all group pipelines`() = runTest {
            val result = Many.of(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 2 }) { _, group -> group }
                .take(1)
                .toList()
                .get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(1, result.value.size)
        }

        @Test
        fun `group Complete is delivered before outer stream Complete`() = runTest {
            // Regression for Bug G1/G3: operator owns subscriptions so this is now structural.
            val completedGroups = mutableSetOf<Int>()
            Many.of(1, 2, 3, 4, 5, 6)
                .groupBy({ it % 3 }) { key, group ->
                    group.doOnComplete { completedGroups.add(key) }.map { key }
                }
                .toList()
                .get()
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
                .get()
            assertEquals(setOf(0, 1), erroredGroups)
        }

        @Test
        fun `groupHandler can apply different transforms per key`() = runTest {
            val result = Many.of(1, 2, 3, 4)
                .groupBy({ it % 2 }) { key, group ->
                    if (key == 0) group.map { it * 10 } else group.map { it * 100 }
                }
                .toList()
                .get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(setOf(20, 40, 100, 300), result.value.toSet())
        }
    }
}
