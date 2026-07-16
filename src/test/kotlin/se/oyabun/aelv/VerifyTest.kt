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

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertEquals

class VerifyTest {

    class EmitsNextTest {

        @Test fun `matches items in order`() {
            Verify.that(Many.items(1, 2, 3))
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test fun `fails on wrong value`() {
            assertFailsWith<IllegalStateException> {
                Verify.that(Many.items(1, 2, 3))
                    .emitsNext(1, 99, 3)
                    .completesNormally()
            }
        }

        @Test fun `fails when stream completes before expected item`() {
            assertFailsWith<Exception> {
                Verify.that(Many.items(1))
                    .emitsNext(1, 2)
                    .completesNormally()
            }
        }
    }

    class EmitsCountTest {

        @Test fun `accepts exactly n items`() {
            Verify.that(Many.items(1, 2, 3))
                .emitsCount(3)
                .completesNormally()
        }

        @Test fun `accepts zero items on empty stream`() {
            Verify.that(Many.empty<Int>())
                .emitsCount(0)
                .completesNormally()
        }
    }

    class AssertNextTest {

        @Test fun `assertion passes for matching item`() {
            Verify.that(Many.items(42))
                .assertNext { assertIs<Int>(it) }
                .completesNormally()
        }

        @Test fun `assertion failure propagates`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.items(1))
                    .assertNext { assertEquals(99, it) }
                    .completesNormally()
            }
        }
    }

    class ThenCancelsTest {

        @Test fun `stops subscription after cancel`() {
            Verify.that(Many.items(1, 2, 3))
                .emitsNext(1)
                .thenCancels()
                .verify()
        }
    }

    class CompletesNormallyTest {

        @Test fun `passes on empty stream`() {
            Verify.that(Many.empty<Int>()).completesNormally()
        }

        @Test fun `passes after all items consumed`() {
            Verify.that(Many.items(1, 2))
                .emitsNext(1, 2)
                .completesNormally()
        }

        @Test fun `fails when stream errors`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.error<Int>(RuntimeException("boom")))
                    .completesNormally()
            }
        }
    }

    class CompletesWithErrorTest {

        @Test fun `returns the error`() {
            val cause = RuntimeException("fail")
            val error = Verify.that(Many.error<Int>(cause)).completesWithError()
            assertEquals(cause, error)
        }

        @Test fun `fails when stream completes normally`() {
            assertFailsWith<Exception> {
                Verify.that(Many.empty<Int>()).completesWithError()
            }
        }
    }

    class IsSubscribedTest {

        @Test fun `checkpoint does not consume items`() {
            Verify.that(Many.items(1, 2))
                .emitsNext(1, 2)
                .completesNormally()
        }
    }

    class IsPresentTest {

        @Test fun `passes when Maybe has value satisfying assertion`() {
            Verify.that(Maybe.present(42))
                .assertNext { assertEquals(42, it) }
                .completesNormally()
        }

        @Test fun `fails when Maybe is empty`() {
            assertFailsWith<Exception> {
                Verify.that(Maybe.empty<Int>())
                    .assertNext { }
                    .completesNormally()
            }
        }
    }

    class IsAbsentTest {

        @Test fun `passes when Maybe is empty`() {
            Verify.that(Maybe.empty<Int>()).isAbsent()
        }

        @Test fun `fails when Maybe has value`() {
            assertFailsWith<Exception> {
                Verify.that(Maybe.present(1)).isAbsent()
            }
        }
    }
}
