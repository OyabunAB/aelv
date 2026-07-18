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
import kotlin.test.assertEquals
import kotlin.test.assertIs

class UnicastSinkTest {

    @Test
    fun `emitted item delivered to single subscriber`() {
        val sink = Sinks.unicast<Int>()
        sink.emit(1)
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1)
            .completesNormally()
    }

    @Test
    fun `multiple items delivered in order`() {
        val sink = Sinks.unicast<Int>()
        (1..5).forEach { sink.emit(it) }
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1, 2, 3, 4, 5)
            .completesNormally()
    }

    @Test
    fun `second subscriber receives error when unicast already has a subscriber`() {
        val sink = Sinks.unicast<Int>()
        (1..4).forEach { sink.emit(it) }
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1, 2, 3, 4)
            .completesNormally()

        Verify.that(sink.asMany()).failedWith<IllegalStateException>()
    }

    @Test
    fun `subscriber receives items emitted before subscription`() {
        val sink = Sinks.unicast<String>()
        sink.emit("hello")
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext("hello")
            .completesNormally()
    }

    @Test
    fun `subscriber receives items emitted after subscription`() {
        val sink    = Sinks.unicast<Int>()
        val emitter = None.defer<Int> { sink.emit(1); sink.emit(2); sink.complete() }.toMany()
        Verify.that(merge(sink.asMany(), emitter))
            .emitsNext(1, 2)
            .completesNormally()
    }

    @Test
    fun `error propagates to subscriber`() {
        val sink  = Sinks.unicast<Int>()
        val cause = RuntimeException("boom")
        sink.error(cause)

        Verify.that(sink.asMany()).failedWith<RuntimeException> {
            assertEquals("boom", it.message)
        }
    }

    @Test
    fun `complete with no items delivers empty stream`() {
        val sink = Sinks.unicast<Int>()
        sink.complete()

        Verify.that(sink.asMany()).completesNormally()
    }

    @Test
    fun `asOne returns first item only`() {
        val sink = Sinks.unicast<String>()
        sink.emit("first")
        sink.emit("second")
        sink.complete()

        Verify.that(sink.asOne())
            .assertNext { assertEquals("first", it) }
            .completesNormally()
    }

    @Test
    fun `emit after complete is ignored`() {
        val sink = Sinks.unicast<Int>()
        sink.complete()
        sink.emit(99)

        Verify.that(sink.asMany()).completesNormally()
    }

    @Test
    fun `emit after error is ignored`() {
        val sink = Sinks.unicast<Int>()
        sink.error(RuntimeException("oops"))
        sink.emit(99)

        Verify.that(sink.asMany()).failedWith<RuntimeException> { assertEquals("oops", it.message) }
    }
}
