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
            val result = Many.items(1, 2, 3).toList().await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(1, 2, 3), result.value)
        }

        @Test
        fun `of iterable emits all items in order`() = runTest {
            val result = Many.from(listOf("a", "b", "c")).toList().await()
            assertIs<Success<List<String>>>(result)
            assertEquals(listOf("a", "b", "c"), result.value)
        }

        @Test
        fun `empty completes with no items`() = runTest {
            val result = Many.empty<Int>().toList().await()
            assertIs<Success<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = Many.error<Int>(cause).toList().await()
            assertIs<Failure<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `cold - each subscriber gets independent execution`() = runTest {
            var count = 0
            val many = Many.generate<Int> { emit ->
                emit(Signal.Upstream.Next(++count))
                emit(Signal.Upstream.Complete)
            }
            val r1 = many.toList().await()
            val r2 = many.toList().await()
            assertIs<Success<List<Int>>>(r1)
            assertIs<Success<List<Int>>>(r2)
            assertEquals(listOf(1), r1.value)
            assertEquals(listOf(2), r2.value)
        }

        @Test
        fun `from flow bridges all items`() = runTest {
            val result = Many.from(kotlinx.coroutines.flow.flowOf(10, 20, 30)).toList().await()
            assertIs<Success<List<Int>>>(result)
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
            val result = One.single(42).await()
            assertIs<Success<Int>>(result)
            assertEquals(42, result.value)
        }

        @Test
        fun `defer executes block on subscribe`() = runTest {
            var executed = false
            val result = One.defer { executed = true; 99 }.await()
            assertIs<Success<Int>>(result)
            assertEquals(99, result.value)
            assertTrue(executed)
        }

        @Test
        fun `cold - executes independently per subscriber`() = runTest {
            var count = 0
            val one = One.defer { ++count }
            one.await()
            one.await()
            assertEquals(2, count)
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = One.error<Int>(cause).await()
            assertIs<Failure<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `asMany emits single item then completes`() = runTest {
            val result = One.single("hello").asMany().toList().await()
            assertIs<Success<List<String>>>(result)
            assertEquals(listOf("hello"), result.value)
        }

        @Test
        fun `create emits value from success callback`() = runTest {
            val result = One.create<Int> { success, _ -> success(42) }.await()
            assertIs<Success<Int>>(result)
            assertEquals(42, result.value)
        }

        @Test
        fun `create signals error from failure callback`() = runTest {
            val cause = RuntimeException("boom")
            val result = One.create<Int> { _, failure -> failure(cause) }.await()
            assertIs<Failure<Exception>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `create only uses first callback invocation`() = runTest {
            val result = One.create<Int> { success, _ -> success(1) }.await()
            assertIs<Success<Int>>(result)
            assertEquals(1, result.value)
        }
    }

    class NoneTest {

        @Test
        fun `complete signals completion`() = runTest {
            assertIs<Success<Unit>>(None.complete<Unit>().await())
        }

        @Test
        fun `defer executes block on subscribe`() = runTest {
            var executed = false
            assertIs<Success<Unit>>(None.defer<Unit> { executed = true }.await())
            assertTrue(executed)
        }

        @Test
        fun `error signals error`() = runTest {
            val cause = InvalidDemandException(-1)
            val result = None.error<Unit>(cause).await()
            assertIs<Failure<AelvException>>(result)
            assertEquals(cause, result.value)
        }

        @Test
        fun `from publisher drains and completes`() = runTest {
            assertIs<Success<Unit>>(None.from(Many.items(1, 2, 3)).await())
        }
        @Test
        fun `source executes exactly once on multiple request calls`() {
            Verify.that(None.complete<Int>().thenReturn(42))
                .assertNext { assertEquals(42, it) }
                .completesNormally()
        }
    }
}

