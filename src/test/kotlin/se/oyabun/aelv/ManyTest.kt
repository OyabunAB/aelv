package se.oyabun.aelv

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

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
    fun `error signals error with no items`() = runTest {
        val cause = UpstreamErrorException(RuntimeException("boom"))
        val result = Many.error<Int>(cause).toList().get()
        assertIs<Either.Right<AelvException>>(result)
        assertEquals(cause, result.value)
    }

    @Test
    fun `cold semantics - each subscriber gets independent execution`() = runTest {
        var count = 0
        val many = Many.generate<Int> { emit ->
            count++
            emit(count)
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
        val flow = kotlinx.coroutines.flow.flowOf(10, 20, 30)
        val result = Many.from(flow).toList().get()
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
        kotlin.test.assertFalse(completed)
    }
}
