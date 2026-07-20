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
                .completes()
        }

        @Test fun `fails on wrong value`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.items(1, 2, 3))
                    .emitsNext(1, 99, 3)
                    .completes()
            }
        }

        @Test fun `fails when stream completes before expected item`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.items(1))
                    .emitsNext(1, 2)
                    .completes()
            }
        }
    }

    class EmitsCountTest {

        @Test fun `accepts exactly n items`() {
            Verify.that(Many.items(1, 2, 3))
                .emitsCount(3)
                .completes()
        }

        @Test fun `accepts zero items on empty stream`() {
            Verify.that(Many.empty<Int>())
                .emitsCount(0)
                .completes()
        }
    }

    class AssertNextTest {

        @Test fun `assertion passes for matching item`() {
            Verify.that(Many.items(42))
                .assertNext { assertIs<Int>(it) }
                .completes()
        }

        @Test fun `assertion failure propagates`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.items(1))
                    .assertNext { assertEquals(99, it) }
                    .completes()
            }
        }
    }

    class AbortedTest {

        @Test fun `aborted verifies upstream cancel signal`() {
            Verify.that(Many.items(1, 2, 3).take(0)).cancels()
        }
    }

    class CompletesNormallyTest {

        @Test fun `passes on empty stream`() {
            Verify.that(Many.empty<Int>()).completes()
        }

        @Test fun `passes after all items consumed`() {
            Verify.that(Many.items(1, 2))
                .emitsNext(1, 2)
                .completes()
        }

        @Test fun `fails when stream errors`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.error<Int>(RuntimeException("boom")))
                    .completes()
            }
        }
    }

    class CompletesWithErrorTest {

        @Test fun `returns the error`() {
            val cause = RuntimeException("fail")
            Verify.that(Many.error<Int>(cause)).failsWith<RuntimeException> {
                assertEquals("fail", it.message)
            }
        }

        @Test fun `fails when stream completes normally`() {
            assertFailsWith<AssertionError> {
                Verify.that(Many.empty<Int>()).fails()
            }
        }
    }

    class CompletedTest {

        @Test fun `completed verifies natural completion signal`() {
            Verify.that(Many.items(1, 2, 3)).emitsNext(1, 2, 3).completes()
        }
    }

    class IsPresentTest {

        @Test fun `passes when Maybe has value satisfying assertion`() {
            Verify.that(Maybe.present(42))
                .assertNext { assertEquals(42, it) }
                .completes()
        }

        @Test fun `fails when Maybe is empty`() {
            assertFailsWith<AssertionError> {
                Verify.that(Maybe.empty<Int>())
                    .assertNext { }
                    .completes()
            }
        }
    }

    class CompletesEmptyTest {

        @Test fun `passes when Maybe is empty`() {
            Verify.that(Maybe.empty<Int>()).emitsCount(0).completes()
        }

        @Test fun `fails when Maybe has value`() {
            assertFailsWith<UnexpectedValueError> {
                Verify.that(Maybe.present(1)).emitsCount(0).completes()
            }
        }
    }
}
