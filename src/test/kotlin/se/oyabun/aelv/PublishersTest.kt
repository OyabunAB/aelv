package se.oyabun.aelv

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class PublishersTest {

    class ManyTest {

        @Test
        fun `of vararg emits all items in order`() = runTest {
            val result = Many.of(1, 2, 3).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `of iterable emits all items in order`() = runTest {
            val result = Many.of(listOf("a", "b", "c")).toList().get()
            assertIs<Either.Left<List<String>>>(result)
            assertEquals(listOf("a", "b", "c"), result.value)
        }

        @Test
        fun `empty completes with no items`() = runTest {
            val result = Many.empty<Int>().toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).toList().get()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `cold - each subscriber gets independent execution`() = runTest {
            var count = 0
            val many = Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(++count))
                emit(Signal.Upstream.Complete)
            }
            val r1 = many.toList().get()
            val r2 = many.toList().get()
            assertIs<Either.Left<List<Int>>>(r1)
            assertIs<Either.Left<List<Int>>>(r2)
            assertEquals(listOf(1), r1.value)
            assertEquals(listOf(2), r2.value)
        }

        @Test
        fun `from flow bridges all items`() = runTest {
            val result = Many.from(kotlinx.coroutines.flow.flowOf(10, 20, 30)).toList().get()
            assertIs<Either.Left<List<Int>>>(result)
            assertEquals(listOf(10, 20, 30), result.value)
        }

        @Test
        fun `never does not complete`() = runTest {
            var completed = false
            val disposable = Many.never<Int>().subscribe(
                prefetch = 1,
                onNext = {},
                onError = {},
                onComplete = { completed = true },
            )
            kotlinx.coroutines.delay(50)
            disposable.cancel()
            assertFalse(completed)
        }
    }

    class OneTest {

        @Test
        fun `of emits value then completes`() = runTest {
            val result = One.of(42).get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(42, result.value)
        }

        @Test
        fun `defer executes block on subscribe`() = runTest {
            var executed = false
            val result = One.defer { executed = true; 99 }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(99, result.value)
            assertTrue(executed)
        }

        @Test
        fun `cold - executes independently per subscriber`() = runTest {
            var count = 0
            val one = One.defer { ++count }
            one.get()
            one.get()
            assertEquals(2, count)
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = One.error<Int>(cause).get()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `asMany emits single item then completes`() = runTest {
            val result = One.of("hello").asMany().toList().get()
            assertIs<Either.Left<List<String>>>(result)
            assertEquals(listOf("hello"), result.value)
        }

        @Test
        fun `create emits value from success callback`() = runTest {
            val result = One.create<Int> { success, _ -> success(42) }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(42, result.value)
        }

        @Test
        fun `create signals error from failure callback`() = runTest {
            val cause = RuntimeException("boom")
            val result = One.create<Int> { _, failure -> failure(cause) }.get()
            assertIs<Either.Right<AelvException>>(result)
            assertIs<UpstreamErrorException>(result.value)
            assertEquals(cause, result.value.cause)
        }

        @Test
        fun `create only uses first callback invocation`() = runTest {
            val result = One.create<Int> { success, _ -> success(1) }.get()
            assertIs<Either.Left<Int>>(result)
            assertEquals(1, result.value)
        }
    }

    class NoneTest {

        @Test
        fun `complete signals completion`() = runTest {
            assertIs<Either.Left<Unit>>(None.complete<Unit>().await())
        }

        @Test
        fun `defer executes block on subscribe`() = runTest {
            var executed = false
            assertIs<Either.Left<Unit>>(None.defer<Unit> { executed = true }.await())
            assertTrue(executed)
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = None.error<Unit>(cause).await()
            assertIs<Either.Right<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `from publisher drains and completes`() = runTest {
            assertIs<Either.Left<Unit>>(None.from(Many.of(1, 2, 3)).await())
        }
    }
}
