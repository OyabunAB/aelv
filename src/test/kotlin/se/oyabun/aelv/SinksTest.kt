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

import kotlinx.coroutines.channels.Channel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
                .completes(within = 5.seconds)
        }

        @Test fun `late subscriber misses earlier emissions`() {
            val sink = Sinks.broadcast<Int>()
            sink.emit(1); sink.emit(2)  // no subscriber yet — lost
            val emitter = Many.defer<Int>(factory = suspend { sink.emit(3); sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().take(1).mergeWith(emitter))
                .emitsNext(3)
                .completes(within = 5.seconds)
        }

        @Test fun `error propagates to all subscribers`() {
            val sink    = Sinks.broadcast<Int>()
            val sub1    = sink.asMany()
            val sub2    = sink.asMany()
            val trigger = None.defer<Int> { sink.error(RuntimeException("boom")) }.toMany()
            Verify.that(merge(sub1, sub2, trigger))
                .failsWith<RuntimeException>(within = 5.seconds) { assertEquals("boom", it.message) }
        }

        @Test fun `complete propagates to all subscribers`() {
            val sink    = Sinks.broadcast<Int>()
            val sub1    = sink.asMany()
            val sub2    = sink.asMany()
            val trigger = None.defer<Int> { sink.complete() }.toMany()
            Verify.that(merge(sub1, sub2, trigger))
                .completes(within = 5.seconds)
        }

        @Test fun `tryEmit returns false after complete`() {
            val sink = Sinks.broadcast<Int>()
            sink.complete()
            assertEquals(false, sink.tryEmit(1))
        }

        @Test fun `tryEmit returns true with no subscribers`() {
            val sink = Sinks.broadcast<Int>()
            assertEquals(true, sink.tryEmit(1))
        }

        @Test fun `asOne returns first available item`() {
            val sink    = Sinks.broadcast<Int>()
            val emitter = Many.defer<Int>(factory = suspend { sink.emit(42); sink.complete(); Many.empty() })
                .delaySubscription(1.milliseconds)
            Verify.that(sink.asOne().toMany().mergeWith(emitter))
                .emitsNext(42)
                .completes(within = 1.seconds)
        }

        @Test fun `slow subscriber is promoted to bounded queue and receives all items`() {
            val bufferSize = 4
            val sink       = Sinks.broadcast<Int>(bufferSize = bufferSize, maxSlowBuffer = 64)
            val emitter    = Many.defer<Int>(factory = suspend {
                repeat(bufferSize + 2) { i -> sink.emit(i) }
                sink.complete()
                Many.empty()
            }).delaySubscription(1.milliseconds)
            Verify.that(sink.asMany().delayElement(20.milliseconds).mergeWith(emitter))
                .emitsCount((bufferSize + 2).toLong())
                .completes(within = 5.seconds)
        }
    }

    class ReplaySinkTest {

        @Test fun `late subscriber receives full history`() {
            val sink = Sinks.replay<Int>()
            sink.emit(1); sink.emit(2); sink.emit(3); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2, 3)
                .completes(within = 1.seconds)
        }

        @Test fun `subscriber receives history then live items`() {
            val sink    = Sinks.replay<Int>()
            sink.emit(1); sink.emit(2)
            val emitter = None.defer<Int> { sink.emit(3); sink.complete() }.toMany()
            Verify.that(sink.asMany().take(3).mergeWith(emitter))
                .emitsNext(1, 2, 3)
                .completes(within = 5.seconds)
        }

        @Test fun `asOne returns first available item`() {
            val sink = Sinks.replay<Int>()
            sink.emit(42); sink.complete()
            Verify.that(sink.asOne())
                .emitsNext(42)
                .completes(within = 1.seconds)
        }

        @Test fun `error replayed to late subscriber`() {
            val sink = Sinks.replay<Int>()
            sink.error(RuntimeException("fail"))
            Verify.that(sink.asMany()).failsWith<RuntimeException>(within = 1.seconds) {
                assertEquals("fail", it.message)
            }
        }

        /**
         * Regression: asMany() must not skip items that were written to the ring buffer
         * after the subscriber captured endPos but before it checks terminal.isSet().
         *
         * Race window:
         *   1. subscriber wakes up, snapshots endPos = N+1 (only item 1 visible)
         *   2. producer writes item 2  (writePos → N+2)
         *   3. producer calls complete (terminal set)
         *   4. subscriber drains item 1, checks terminal → set → emits Complete
         *      item 2 at ring buffer position N+1 is never delivered  ← BUG
         *
         * The fix: only check terminal when drained=false (nothing new was read);
         * when drained=true, loop back unconditionally to re-read writePos first.
         */
        @Test fun `does not skip items written to ring buffer between endPos snapshot and terminal check`() {
            // Gate channels used to pin the subscriber inside generatorEmit(item 1)
            // while the producer writes item 2 + complete on a concurrent coroutine.
            val afterItem1 = Channel<Unit>(Channel.RENDEZVOUS)
            val canContinue = Channel<Unit>(Channel.RENDEZVOUS)

            val sink = Sinks.replay<Int>()

            // Consumer: pauses inside the item-1 callback so the producer can
            // write item 2 and call complete while generatorEmit(1) is still running.
            val consumer = sink.asMany().doOnNext { item ->
                if (item == 1) {
                    afterItem1.send(Unit)   // signal: I am inside generatorEmit(item 1)
                    canContinue.receive()  // wait: producer has written item 2 + complete
                }
            }

            // Producer: emits 1, waits until the consumer is paused inside generatorEmit(1),
            // then writes item 2 and completes the sink — exactly the race window.
            val producer = None.defer<Int> {
                sink.tryEmit(1)
                afterItem1.receive()  // consumer is now paused inside generatorEmit(item 1)
                sink.tryEmit(2)       // writePos advances; item 2 now in ring buffer
                sink.complete()       // terminal set — this is the dangerous moment
                canContinue.send(Unit)
            }.toMany()

            Verify.that(merge(consumer, producer))
                .emitsNext(1, 2)
                .completes(within = 2.seconds)
        }
    }

    class ReplayLastSinkTest {

        @Test fun `late subscriber receives only last n items`() {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1); sink.emit(2); sink.emit(3); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(2, 3)
                .completes(within = 1.seconds)
        }

        @Test fun `window slides as new items arrive`() {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1); sink.emit(2); sink.emit(3); sink.emit(4); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(3, 4)
                .completes(within = 1.seconds)
        }

        @Test fun `fewer items than window replays all`() {
            val sink = Sinks.replayLast<Int>(5)
            sink.emit(1); sink.emit(2); sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completes(within = 1.seconds)
        }

        @Test fun `count zero is rejected`() {
            assertFailsWith<IllegalArgumentException> { Sinks.replayLast<Int>(0) }
        }

        @Test fun `asOne returns first item of replay window`() {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1); sink.emit(2); sink.emit(3); sink.complete()
            // replayLast(2) replays [2, 3]; asOne() = first() = 2
            Verify.that(sink.asOne())
                .emitsNext(2)
                .completes(within = 1.seconds)
        }
    }
}
