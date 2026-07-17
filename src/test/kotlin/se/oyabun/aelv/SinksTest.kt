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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SinksTest {

    class BroadcastSinkTest {

        @Test fun `emits to active subscriber`() {
            val sink    = Sinks.broadcast<Int>()
            val emitter = Many.items(1, 2, 3)
                .delaySubscription(1.milliseconds)
                .flatMap { v: Int -> sink.emit(v); Many.empty<Int>() }
                .concatWith(Many.defer<Int>(factory = suspend { sink.complete(); Many.empty() }))
            Verify.that(sink.asMany().take(3).mergeWith(emitter))
                .emitsCount(3)
                .completesNormally(within = 5.seconds)
        }

        @Test fun `late subscriber misses earlier emissions`() {
            val sink = Sinks.broadcast<Int>()
            sink.emit(1); sink.emit(2)  // no subscriber yet — lost
            val emitter = Many.defer<Int>(factory = suspend { sink.emit(3); sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().take(1).mergeWith(emitter))
                .emitsNext(3)
                .completesNormally(within = 5.seconds)
        }

        @Test fun `error propagates to all subscribers`() {
            val sink    = Sinks.broadcast<Int>()
            val trigger = Many.defer<Int>(factory = suspend { sink.error(RuntimeException("boom")); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().mergeWith(trigger))
                .completesWithError(within = 5.seconds)
        }

        @Test fun `complete propagates to all subscribers`() {
            val sink    = Sinks.broadcast<Int>()
            val trigger = Many.defer<Int>(factory = suspend { sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().mergeWith(trigger))
                .completesNormally(within = 5.seconds)
        }

        @Test fun `tryEmit returns false after complete`() {
            val sink = Sinks.broadcast<Int>()
            sink.complete()
            assertEquals(false, sink.tryEmit(1))
        }
    }

    class ReplaySinkTest {

        @Test fun `late subscriber receives full history`() {
            val sink = Sinks.replay<Int>()
            sink.emit(1); sink.emit(2); sink.emit(3); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2, 3)
                .completesNormally(within = 1.seconds)
        }

        @Test fun `subscriber receives history then live items`() {
            val sink    = Sinks.replay<Int>()
            sink.emit(1); sink.emit(2)
            val emitter = Many.defer<Int>(factory = suspend { sink.emit(3); sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().take(3).mergeWith(emitter))
                .emitsCount(3)
                .completesNormally(within = 5.seconds)
        }

        @Test fun `asOne returns first available item`() {
            val sink = Sinks.replay<Int>()
            sink.emit(42); sink.complete()
            Verify.that(sink.asOne())
                .emitsNext(42)
                .completesNormally(within = 1.seconds)
        }

        @Test fun `error replayed to late subscriber`() {
            val sink = Sinks.replay<Int>()
            sink.error(RuntimeException("fail"))
            val error = Verify.that(sink.asMany()).completesWithError(within = 1.seconds)
            assertEquals("fail", error.message)
        }
    }

    class ReplayLastSinkTest {

        @Test fun `late subscriber receives only last n items`() {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1); sink.emit(2); sink.emit(3); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(2, 3)
                .completesNormally(within = 1.seconds)
        }

        @Test fun `window slides as new items arrive`() {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1); sink.emit(2); sink.emit(3); sink.emit(4); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(3, 4)
                .completesNormally(within = 1.seconds)
        }

        @Test fun `fewer items than window replays all`() {
            val sink = Sinks.replayLast<Int>(5)
            sink.emit(1); sink.emit(2); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally(within = 1.seconds)
        }

        @Test fun `count zero is rejected`() {
            try {
                Sinks.replayLast<Int>(0)
                error("expected IllegalArgumentException")
            } catch (e: IllegalArgumentException) {
                assertIs<IllegalArgumentException>(e)
            }
        }
    }

    class UnicastSinkTest {

        @Test fun `emits items to subscriber`() {
            val sink    = Sinks.unicast<Int>()
            val emitter = Many.items(1, 2, 3)
                .delaySubscription(1.milliseconds)
                .flatMap { v: Int -> sink.emit(v); Many.empty<Int>() }
                .concatWith(Many.defer<Int>(factory = suspend { sink.complete(); Many.empty() }))
            Verify.that(sink.asMany().take(3).mergeWith(emitter))
                .emitsCount(3)
                .completesNormally(within = 5.seconds)
        }

        @Test fun `asOne receives next available item`() {
            val sink    = Sinks.unicast<Int>()
            val emitter = Many.defer<Int>(factory = suspend { sink.emit(99); sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asOne().toMany().mergeWith(emitter))
                .emitsNext(99)
                .completesNormally(within = 5.seconds)
        }

        @Test fun `error propagates to subscriber`() {
            val sink    = Sinks.unicast<Int>()
            val emitter = Many.defer<Int>(factory = suspend { sink.error(RuntimeException("fail")); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().mergeWith(emitter))
                .completesWithError(within = 5.seconds)
        }

        @Test fun `emit after complete is ignored`() {
            val sink = Sinks.unicast<Int>()
            sink.complete()
            Verify.that(sink.asMany()).completesNormally(within = 1.seconds)
        }

        @Test fun `items emitted before subscription are delivered`() {
            val sink = Sinks.unicast<Int>()
            sink.emit(10); sink.emit(20); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(10, 20)
                .completesNormally(within = 1.seconds)
        }
    }
}
