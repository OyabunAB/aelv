package se.oyabun.aelv

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

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
                emit(42)
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
            }.retry(times = 3).get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(42, result.value)
            assertEquals(3, attempts)
        }
    }
}
