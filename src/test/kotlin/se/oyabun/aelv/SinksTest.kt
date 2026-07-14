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
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class SinksTest {

    class BroadcastSinkTest {

        @Test fun `emits to active subscriber`() = runTest {
            val sink      = Sinks.broadcast<Int>()
            val subscribed = Channel<Unit>(1)
            val job = launch {
                val stream = sink.asMany()
                subscribed.send(Unit)
                Verify.that(stream).emitsNext(1, 2, 3).completesNormally()
            }
            subscribed.receive()
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            job.join()
        }

        @Test fun `late subscriber misses earlier emissions`() = runTest {
            val sink = Sinks.broadcast<Int>()
            sink.emit(1)
            sink.emit(2)
            val subscribed = Channel<Unit>(1)
            val job = launch {
                val stream = sink.asMany()
                subscribed.send(Unit)
                Verify.that(stream).emitsNext(3).completesNormally()
            }
            subscribed.receive()
            sink.emit(3)
            sink.complete()
            job.join()
        }

        @Test fun `error propagates to all subscribers`() = runTest {
            val sink  = Sinks.broadcast<Int>()
            val ready = Channel<Unit>(2)
            val job1  = launch { val s = sink.asMany(); ready.send(Unit); Verify.that(s).completesWithError() }
            val job2  = launch { val s = sink.asMany(); ready.send(Unit); Verify.that(s).completesWithError() }
            ready.receive(); ready.receive()
            sink.error(RuntimeException("boom"))
            job1.join(); job2.join()
        }

        @Test fun `complete propagates to all subscribers`() = runTest {
            val sink  = Sinks.broadcast<Int>()
            val ready = Channel<Unit>(2)
            val job1  = launch { val s = sink.asMany(); ready.send(Unit); Verify.that(s).completesNormally() }
            val job2  = launch { val s = sink.asMany(); ready.send(Unit); Verify.that(s).completesNormally() }
            ready.receive(); ready.receive()
            sink.complete()
            job1.join(); job2.join()
        }

        @Test fun `tryEmit returns false after complete`() = runTest {
            val sink = Sinks.broadcast<Int>()
            sink.complete()
            assertEquals(false, sink.tryEmit(1))
        }
    }

    class ReplaySinkTest {

        @Test fun `late subscriber receives full history`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2, 3)
                .completesNormally()
        }

        @Test fun `subscriber receives history then live items`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(1)
            sink.emit(2)
            val job = launch {
                Verify.that(sink.asMany()).emitsNext(1, 2, 3).completesNormally()
            }
            sink.emit(3)
            sink.complete()
            job.join()
        }

        @Test fun `asOne returns first available item`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.emit(42)
            sink.complete()
            Verify.that(sink.asOne()).emitsNext(42).completesNormally()
        }

        @Test fun `error replayed to late subscriber`() = runTest {
            val sink = Sinks.replay<Int>()
            sink.error(RuntimeException("fail"))
            val error = Verify.that(sink.asMany()).completesWithError()
            assertEquals("fail", error.message)
        }
    }

    class ReplayLastSinkTest {

        @Test fun `late subscriber receives only last n items`() = runTest {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(2, 3)
                .completesNormally()
        }

        @Test fun `window slides as new items arrive`() = runTest {
            val sink = Sinks.replayLast<Int>(2)
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.emit(4)
            sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(3, 4)
                .completesNormally()
        }

        @Test fun `fewer items than window replays all`() = runTest {
            val sink = Sinks.replayLast<Int>(5)
            sink.emit(1)
            sink.emit(2)
            sink.complete()
            Verify.that(sink.asMany())
                .emitsNext(1, 2)
                .completesNormally()
        }

        @Test fun `count zero is rejected`() {
            try {
                Sinks.replayLast<Int>(0)
                error("expected IllegalArgumentException")
            } catch (exception: IllegalArgumentException) {
                assertIs<IllegalArgumentException>(exception)
            }
        }
    }

    class UnicastSinkTest {

        @Test fun `emits items to subscriber`() = runTest {
            val sink = Sinks.unicast<Int>()
            val job  = launch { Verify.that(sink.asMany()).emitsNext(1, 2, 3).completesNormally() }
            sink.emit(1)
            sink.emit(2)
            sink.emit(3)
            sink.complete()
            job.join()
        }

        @Test fun `asOne receives next available item`() = runTest {
            val sink = Sinks.unicast<Int>()
            val job  = launch { Verify.that(sink.asOne()).emitsNext(99).completesNormally() }
            sink.emit(99)
            sink.complete()
            job.join()
        }

        @Test fun `error propagates to subscriber`() = runTest {
            val sink = Sinks.unicast<Int>()
            val job  = launch { Verify.that(sink.asMany()).completesWithError() }
            sink.error(RuntimeException("fail"))
            job.join()
        }

        @Test fun `emit after complete is ignored`() = runTest {
            val sink = Sinks.unicast<Int>()
            sink.complete()
            sink.emit(1)
            Verify.that(sink.asMany()).completesNormally()
        }

        @Test fun `items emitted before subscription are delivered`() = runTest {
            val sink = Sinks.unicast<Int>()
            sink.emit(10)
            sink.emit(20)
            sink.complete()
            Verify.that(sink.asMany()).emitsNext(10, 20).completesNormally()
        }
    }
}
