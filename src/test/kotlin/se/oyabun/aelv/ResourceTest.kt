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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ResourceTest {

    private fun <T> trackingRelease(
        released: AtomicBoolean,
        signal: AtomicReference<Either<Throwable, Unit>>,
    ): (T, Either<Throwable, Unit>) -> None<Unit> = { _, s ->
        released.set(true)
        signal.set(s)
        None.defer<Unit> { }
    }

    class ManyResourceTest {

        @Test
        fun `emits all items from use and releases with success`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            Verify.that(
                Many.resource(
                    acquire = { One.single("db") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> Many.items(1, 2, 3) },
                )
            )
                .emitsNext(1, 2, 3)
                .completes()
            assertTrue(released.get(), "release must be called on normal completion")
            assertIs<Success<Unit>>(releaseSignal.get(), "release signal must be Success on normal completion")
        }

        @Test
        fun `release is called with failure when use errors`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            val cause = InvalidDemandException(-1)
            Verify.that(
                Many.resource(
                    acquire = { One.single("db") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> Many.error(cause) },
                )
            )
                .failsWith<InvalidDemandException>()
            assertTrue(released.get(), "release must be called on error")
            val sig = releaseSignal.get()
            assertIs<Failure<Throwable>>(sig, "release signal must be Failure on error")
            assertEquals(cause, sig.value)
        }

        @Test
        fun `release is called when downstream cancels`() {
            val released = AtomicBoolean(false)
            Verify.that(
                Many.resource(
                    acquire = { One.single("db") },
                    release = { _, _ -> released.set(true); None.defer<Unit> { } },
                    use     = { _ -> Many.items(1, 2, 3) },
                )
                    .take(1)
            )
                .emitsNext(1)
                .completes()
            assertTrue(released.get(), "release must be called when downstream cancels")
        }

        @Test
        fun `acquire error propagates without calling release`() {
            val released = AtomicBoolean(false)
            val cause = InvalidDemandException(-99)
            Verify.that(
                Many.resource(
                    acquire = { One.error(cause) },
                    release = { _, _ -> released.set(true); None.defer<Unit> { } },
                    use     = { _ -> Many.items(1) },
                )
            )
                .failsWith<InvalidDemandException>()
        }

        @Test
        fun `release is called when use throws`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            val cause = InvalidDemandException(-99)
            Verify.that(
                Many.resource(
                    acquire = { One.single("db") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> throw cause },
                )
            )
                .failsWith<InvalidDemandException>()
            assertTrue(released.get(), "release must be called when use() throws")
            assertIs<Failure<Throwable>>(releaseSignal.get())
            assertEquals(cause, (releaseSignal.get() as Failure).value)
        }
    }

    class OneResourceTest {

        @Test
        fun `emits single value and releases with success`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            Verify.that(
                One.resource(
                    acquire = { One.single("conn") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> One.single(42) },
                )
            )
                .assertNext { assertEquals(42, it) }
                .completes()
            assertTrue(released.get(), "release must be called on normal completion")
            assertIs<Success<Unit>>(releaseSignal.get())
        }

        @Test
        fun `release is called with failure when use errors`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            val cause = InvalidDemandException(-1)
            Verify.that(
                One.resource(
                    acquire = { One.single("conn") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> One.error(cause) },
                )
            )
                .failsWith<InvalidDemandException>()
            assertTrue(released.get(), "release must be called on error")
            val sig = releaseSignal.get()
            assertIs<Failure<Throwable>>(sig)
            assertEquals(cause, sig.value)
        }

        @Test
        fun `acquire error propagates`() {
            val cause = InvalidDemandException(-99)
            Verify.that(
                One.resource(
                    acquire = { One.error(cause) },
                    release = { _, _ -> None.defer<Unit> { } },
                    use     = { _ -> One.single(1) },
                )
            )
                .failsWith<InvalidDemandException>()
        }
    }

    class MaybeResourceTest {

        @Test
        fun `emits present value and releases with success`() {
            val released = AtomicBoolean(false)
            Verify.that(
                Maybe.resource(
                    acquire = { One.single("conn") },
                    release = { _, _ -> released.set(true); None.defer<Unit> { } },
                    use     = { _ -> Maybe.present(7) },
                )
            )
                .assertNext { assertEquals(7, it) }
                .completes()
            assertTrue(released.get())
        }

        @Test
        fun `completes empty when use is empty and releases`() {
            val released = AtomicBoolean(false)
            Verify.that(
                Maybe.resource(
                    acquire = { One.single("conn") },
                    release = { _, _ -> released.set(true); None.defer<Unit> { } },
                    use     = { _ -> Maybe.empty() },
                )
            )
                .emitsCount(0)
                .completes()
            assertTrue(released.get(), "release must be called even when Maybe is empty")
        }

        @Test
        fun `release is called with failure when use errors`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            val cause = InvalidDemandException(-1)
            Verify.that(
                Maybe.resource(
                    acquire = { One.single("conn") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> Maybe.error(cause) },
                )
            )
                .failsWith<InvalidDemandException>()
            assertTrue(released.get())
            assertIs<Failure<Throwable>>(releaseSignal.get())
        }
    }

    class NoneResourceTest {

        @Test
        fun `runs side-effect and releases with success`() {
            val effectRan = AtomicBoolean(false)
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            Verify.that(
                None.resource(
                    acquire = { One.single("conn") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> None.defer { effectRan.set(true) } },
                ).toMany()
            )
                .completes()
            assertTrue(effectRan.get(), "side-effect must run")
            assertTrue(released.get(), "release must be called")
            assertIs<Success<Unit>>(releaseSignal.get())
        }

        @Test
        fun `release is called with failure when use errors`() {
            val released = AtomicBoolean(false)
            val releaseSignal = AtomicReference<Either<Throwable, Unit>>()
            val cause = InvalidDemandException(-1)
            Verify.that(
                None.resource(
                    acquire = { One.single("conn") },
                    release = { _, s -> released.set(true); releaseSignal.set(s); None.defer<Unit> { } },
                    use     = { _ -> None.error(cause) },
                ).toMany()
            )
                .failsWith<InvalidDemandException>()
            assertTrue(released.get())
            assertIs<Failure<Throwable>>(releaseSignal.get())
            assertEquals(cause, (releaseSignal.get() as Failure).value)
        }
    }
}
