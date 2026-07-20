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
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class MaybeTest {

    class FactoriesTest {

        @Test fun `present emits value and completes`() {
            Verify.that(Maybe.present(42))
                .assertNext { assertEquals(42, it) }
                .completes()
        }

        @Test fun `empty completes without emitting`() {
            Verify.that(Maybe.empty<Int>()).emitsCount(0).completes()
        }

        @Test fun `error propagates`() {
            Verify.that(Maybe.error<Int>(RuntimeException("boom")))
                .failsWith<RuntimeException> { assertEquals("boom", it.message) }
        }

        @Test fun `defer with non-null result produces present`() {
            Verify.that(Maybe.defer { 42 })
                .assertNext { assertEquals(42, it) }
                .completes()
        }

        @Test fun `defer with null result produces empty`() {
            Verify.that(Maybe.defer<Int> { null }).emitsCount(0).completes()
        }

        @Test fun `defer with exception propagates as error`() {
            Verify.that(Maybe.defer<Int> { throw RuntimeException("fail") })
                .failsWith<RuntimeException> { assertEquals("fail", it.message) }
        }

        @Test fun `One toMaybe produces present`() {
            Verify.that(One.single(99).toMaybe())
                .assertNext { assertEquals(99, it) }
                .completes()
        }

        @Test fun `firstMaybe on non-empty Many produces present with first item`() {
            Verify.that(Many.items(1, 2, 3).firstMaybe())
                .assertNext { assertEquals(1, it) }
                .completes()
        }

        @Test fun `firstMaybe on empty Many produces empty`() {
            Verify.that(Many.empty<Int>().firstMaybe()).emitsCount(0).completes()
        }
    }

    class MapTest {

        @Test fun `transforms present value`() {
            Verify.that(Maybe.present(3).map { it * 2 })
                .assertNext { assertEquals(6, it) }
                .completes()
        }

        @Test fun `on empty stays empty`() {
            Verify.that(Maybe.empty<Int>().map { it * 2 }).emitsCount(0).completes()
        }

        @Test fun `suspend variant transforms value`() = runTest {
            Verify.that(Maybe.present("hello").map(transform = suspend { s: String -> s.length }))
                .assertNext { assertEquals(5, it) }
                .completes()
        }
    }

    class FilterTest {

        @Test fun `keeps value when predicate matches`() {
            Verify.that(Maybe.present(10).filter { it > 5 })
                .assertNext { assertEquals(10, it) }
                .completes()
        }

        @Test fun `removes value when predicate does not match`() {
            Verify.that(Maybe.present(3).filter { it > 5 }).emitsCount(0).completes()
        }

        @Test fun `on empty stays empty`() {
            Verify.that(Maybe.empty<Int>().filter { it > 5 }).emitsCount(0).completes()
        }
    }

    class FlatMapTest {

        @Test fun `present to present`() {
            Verify.that(Maybe.present(2).flatMap { value: Int -> Maybe.present(value * 10) })
                .assertNext { assertEquals(20, it) }
                .completes()
        }

        @Test fun `present to empty`() {
            Verify.that(Maybe.present(2).flatMap { _: Int -> Maybe.empty<Int>() }).emitsCount(0).completes()
        }

        @Test fun `empty stays empty`() {
            Verify.that(Maybe.empty<Int>().flatMap { value: Int -> Maybe.present(value * 10) }).emitsCount(0).completes()
        }

        @Test fun `flatMapMany expands to Many`() {
            Verify.that(Maybe.present(3).flatMapMany { value: Int -> Many.items(value, value + 1, value + 2) })
                .emitsNext(3, 4, 5)
                .completes()
        }

        @Test fun `flatMapMany on empty produces empty Many`() {
            Verify.that(Maybe.empty<Int>().flatMapMany { value: Int -> Many.items(value) })
                .completes()
        }

        @Test fun `flatMapNone runs side effect when present`() {
            var ran = false
            Verify.that(Maybe.present(1).flatMapNone { _: Int -> None.defer<Any> { ran = true } })
                .completes()
            assertEquals(true, ran)
        }

        @Test fun `flatMapNone does not run when empty`() {
            var ran = false
            Verify.that(Maybe.empty<Int>().flatMapNone { _: Int -> None.defer<Any> { ran = true } })
                .completes()
            assertEquals(false, ran)
        }
    }

    class OrTest {

        @Test fun `on present returns present value`() {
            Verify.that(Maybe.present(7).or { 99 })
                .emitsNext(7)
                .completes()
        }

        @Test fun `on empty returns fallback value`() {
            Verify.that(Maybe.empty<Int>().or { 99 })
                .emitsNext(99)
                .completes()
        }

        @Test fun `suspend variant returns fallback`() = runTest {
            Verify.that(Maybe.empty<Int>().or(fallback = suspend { 42 }))
                .emitsNext(42)
                .completes()
        }
    }

    class OrManyTest {

        @Test fun `on present ignores fallback stream`() {
            Verify.that(Maybe.present(1).orMany { Many.items(10, 20) })
                .emitsNext(1)
                .completes()
        }

        @Test fun `on empty switches to fallback stream`() {
            Verify.that(Maybe.empty<Int>().orMany { Many.items(10, 20) })
                .emitsNext(10, 20)
                .completes()
        }
    }

    class ConversionTest {

        @Test fun `toMany on present emits one item`() {
            Verify.that(Maybe.present(5).toMany())
                .emitsNext(5)
                .completes()
        }

        @Test fun `toMany on empty completes without items`() {
            Verify.that(Maybe.empty<Int>().toMany()).completes()
        }

        @Test fun `toOne on present emits value`() {
            Verify.that(Maybe.present(3).toOne())
                .emitsNext(3)
                .completes()
        }

        @Test fun `toOne on empty throws NoSuchElementException`() {
            Verify.that(Maybe.empty<Int>().toOne()).failsWith<NoSuchElementException>()
        }
    }

    class AwaitTest {

        @Test fun `on present returns value`() = runTest {
            val result = Maybe.present(42).await()
            assertEquals(42, (result as Success).value)
        }

        @Test fun `on empty returns null`() = runTest {
            val result = Maybe.empty<Int>().await()
            assertNull((result as Success).value)
        }

        @Test fun `on error returns failure`() = runTest {
            val result = Maybe.error<Int>(RuntimeException("oops")).await()
            assertIs<Failure<*>>(result)
        }
    }

    class RecoverTest {

        @Test fun `replaces error with fallback Maybe`() {
            Verify.that(Maybe.error<Int>(RuntimeException("bad")).recover { Maybe.present(0) })
                .assertNext { assertEquals(0, it) }
                .completes()
        }

        @Test fun `passes through non-error`() {
            Verify.that(Maybe.present(5).recover { Maybe.present(0) })
                .assertNext { assertEquals(5, it) }
                .completes()
        }
    }

    class FusionTest {

        @Test fun `map uses fusion fast path on fused source`() = runTest {
            val result = Many.items(7).toMaybe().map { it * 3 }.await()
            assertIs<Success<Int?>>(result)
            assertEquals(21, result.value)
        }

        @Test fun `filter uses fusion fast path on fused source`() = runTest {
            val present = Many.items(5).toMaybe().filter { it > 0 }.await()
            assertIs<Success<Int?>>(present)
            assertEquals(5, present.value)

            val absent = Many.items(5).toMaybe().filter { it > 10 }.await()
            assertIs<Success<Int?>>(absent)
            assertNull(absent.value)
        }

        @Test fun `await returns null for empty fused source`() = runTest {
            val result = Many.empty<Int>().toMaybe().await()
            assertIs<Success<Int?>>(result)
            assertNull(result.value)
        }

        @Test fun `toMany propagates fusion`() = runTest {
            val result = Many.range(1, 3).toMaybe().map { it * 2 }.toMany().toList().await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(2), result.value)
        }
    }
}
