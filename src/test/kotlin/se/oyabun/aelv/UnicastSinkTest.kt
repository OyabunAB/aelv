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
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

class UnicastSinkTest {

    @Test
    fun `emitted item delivered to single subscriber`() {
        val sink = Sinks.unicast<Int>()
        sink.emit(1)
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1)
            .completes()
    }

    @Test
    fun `multiple items delivered in order`() {
        val sink = Sinks.unicast<Int>()
        (1..5).forEach { sink.emit(it) }
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1, 2, 3, 4, 5)
            .completes()
    }

    @Test
    fun `second subscriber receives error when unicast already has a subscriber`() {
        val sink = Sinks.unicast<Int>()
        (1..4).forEach { sink.emit(it) }
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext(1, 2, 3, 4)
            .completes()

        Verify.that(sink.asMany()).failsWith<IllegalStateException>()
    }

    @Test
    fun `subscriber receives items emitted before subscription`() {
        val sink = Sinks.unicast<String>()
        sink.emit("hello")
        sink.complete()

        Verify.that(sink.asMany())
            .emitsNext("hello")
            .completes()
    }

    @Test
    fun `subscriber receives items emitted after subscription`() {
        val sink    = Sinks.unicast<Int>()
        val emitter = None.defer<Int> { sink.emit(1); sink.emit(2); sink.complete() }.toMany()
        Verify.that(Many.merge(sink.asMany(), emitter))
            .emitsNext(1, 2)
            .completes()
    }

    @Test
    fun `error propagates to subscriber`() {
        val sink  = Sinks.unicast<Int>()
        val cause = RuntimeException("boom")
        sink.error(cause)

        Verify.that(sink.asMany()).failsWith<RuntimeException> {
            assertEquals("boom", it.message)
        }
    }

    @Test
    fun `complete with no items delivers empty stream`() {
        val sink = Sinks.unicast<Int>()
        sink.complete()

        Verify.that(sink.asMany()).completes()
    }

    @Test
    fun `asOne returns first item only`() {
        val sink = Sinks.unicast<String>()
        sink.emit("first")
        sink.emit("second")
        sink.complete()

        Verify.that(sink.asOne())
            .assertNext { assertEquals("first", it) }
            .completes()
    }

    @Test
    fun `emit after complete is ignored`() {
        val sink = Sinks.unicast<Int>()
        sink.complete()
        sink.emit(99)

        Verify.that(sink.asMany()).completes()
    }

    @Test
    fun `emit after error is ignored`() {
        val sink = Sinks.unicast<Int>()
        sink.error(RuntimeException("oops"))
        sink.emit(99)

        Verify.that(sink.asMany()).failsWith<RuntimeException> { assertEquals("oops", it.message) }
    }

    @Test
    fun `onRequest fires with the requested demand count`() {
        val demands = Sinks.unicast<Long>()
        val sink    = Sinks.unicast<Int>().onRequest { n -> demands.emit(n) }
        sink.emit(1, 2, 3).complete()

        Verify.that(
            Many.from(sink.asMany() as Publisher<Int>)
                .doOnComplete { demands.complete() }
                .discard()
                .andThen { demands.asMany() },
        ).assertNext { assertTrue(it > 0L, "demand must be positive, got $it") }
            .completes(within = 2.seconds)
    }

    @Test
    fun `onRequest receives exact n from a raw subscriber`() {
        val demands = Sinks.unicast<Long>()
        val sink    = Sinks.unicast<Int>().onRequest { n -> demands.emit(n) }
        sink.emit(1, 2, 3).complete()

        sink.asMany().subscribe(object : Subscriber<Int> {
            override fun onSubscribe(s: Subscription) { s.request(2) }
            override fun onNext(value: Int) = Unit
            override fun onError(t: Throwable) = Unit
            override fun onComplete() = Unit
        })

        Verify.that(
            None.defer<Unit> { delay(100.milliseconds); demands.complete() }
                .andThen { demands.asMany() },
        ).emitsNext(2L).completes(within = 2.seconds)
    }

    @Test
    fun `onRequest propagates through operator chains`() {
        val demands = Sinks.unicast<Long>()
        val sink    = Sinks.unicast<Int>().onRequest { n -> demands.emit(n) }
        sink.emit(1, 2, 3).complete()

        Verify.that(
            Many.from(sink.asMany().map { it * 2 } as Publisher<Int>)
                .doOnComplete { demands.complete() }
                .discard()
                .andThen { demands.asMany() },
        ).assertNext { assertTrue(it > 0L, "demand must be positive, got $it") }
            .completes(within = 2.seconds)
    }
}
