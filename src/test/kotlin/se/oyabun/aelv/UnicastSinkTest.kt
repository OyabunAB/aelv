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

import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

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
    fun `each item delivered to exactly one of two competing subscribers`() = runTest {
        val sink     = Sinks.unicast<Int>()
        val received = mutableListOf<Int>()

        val job1 = launch { sink.asMany().toList().await().getOrThrow().let { received += it } }
        val job2 = launch { sink.asMany().toList().await().getOrThrow().let { received += it } }

        withTimeout(2.seconds) {
            (1..4).forEach { sink.emit(it) }
            sink.complete()
            job1.join()
            job2.join()
        }

        assertEquals(setOf(1, 2, 3, 4), received.toSet())
        assertEquals(4, received.size)
    }

    @Test
    fun `subscriber receives items emitted after subscription`() = runTest {
        val sink = Sinks.unicast<String>()
        var received: String? = null

        val job = launch {
            received = sink.asOne().await().getOrThrow()
        }

        withTimeout(2.seconds) {
            sink.emit("hello")
            job.join()
        }

        assertEquals("hello", received)
    }

    @Test
    fun `error propagates to subscriber`() {
        val sink  = Sinks.unicast<Int>()
        val cause = RuntimeException("boom")
        sink.error(cause)

        val thrown = Verify.that(sink.asMany()).completesWithError()
        assertIs<RuntimeException>(thrown)
        assertEquals("boom", thrown.message)
    }

    @Test
    fun `complete with no items delivers empty stream`() {
        val sink = Sinks.unicast<Int>()
        sink.complete()

        Verify.that(sink.asMany()).completesNormally()
    }

    @Test
    fun `asOne returns One emitting the next available item`() {
        val sink = Sinks.unicast<String>()
        sink.emit("first")
        sink.emit("second")

        Verify.that(sink.asOne())
            .emitsNext("first")
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

        Verify.that(sink.asMany()).completesWithError()
    }
}
