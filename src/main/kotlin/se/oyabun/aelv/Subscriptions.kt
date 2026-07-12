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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

internal sealed interface SubscriptionState {
    data object Unbound                      : SubscriptionState
    data class  Bound(val subscription: Subscription) : SubscriptionState
}

private val dispatcher = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()) { r ->
    Thread(r, "aelv-worker").also { it.isDaemon = true }
}.asCoroutineDispatcher()

private val sharedScope = CoroutineScope(dispatcher + SupervisorJob())

private val noopJob: Job = Job().also { it.cancel() }

internal class StreamSubscription<T : Any>(
    private val subscriber: Subscriber<in T>,
    private val source: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (Exception) -> Unit,
    ) -> Unit,
) : Subscription {

    private val log  = Logging.of<StreamSubscription<*>>()
    private val name = subscriber.javaClass.simpleName.ifEmpty { "Subscriber" }

    private val demand     = AtomicLong(0L)
    private val signal     = Channel<Unit>(Channel.UNLIMITED)
    private val terminated = AtomicBoolean(false)
    private val started    = AtomicBoolean(false)
    private val producer   = AtomicReference(noopJob)

    private fun start() {
        log.stream.subscribing(name)
        val producerJob = sharedScope.launch {
            try {
                source(
                    { value ->
                        awaitDemand()
                        if (terminated.get()) return@source Signal.Downstream.Cancel
                        demand.updateAndGet { d -> if (d == Long.MAX_VALUE) d else d - 1 }
                        subscriber.onNext(value)
                        if (terminated.get()) Signal.Downstream.Cancel
                        else Signal.Downstream.Request
                    },
                    {
                        if (terminated.compareAndSet(false, true)) {
                            log.stream.completed(name)
                            subscriber.onComplete()
                        }
                        shutdown()
                    },
                    { cause ->
                        if (terminated.compareAndSet(false, true)) {
                            log.stream.error(name, cause)
                            subscriber.onError(cause)
                        }
                        shutdown()
                    },
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (terminated.compareAndSet(false, true)) {
                    log.stream.error(name, e)
                    subscriber.onError(e)
                }
                shutdown()
            }
        }
        producer.set(producerJob)
        if (terminated.get()) producerJob.cancel()
    }

    private fun shutdown() {
        signal.close()
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                producer.get().cancel()
                subscriber.onError(InvalidDemandException(n))
                shutdown()
            }
            return
        }
        log.subscription.requested(name, n)
        demand.updateAndGet { current ->
            if (current >= Long.MAX_VALUE / 2) Long.MAX_VALUE else current + n
        }
        signal.trySend(Unit)
        if (started.compareAndSet(false, true)) start()
    }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            log.stream.cancelled(name)
            producer.get().cancel()
            shutdown()
        }
    }

    private suspend fun awaitDemand() {
        while (!terminated.get()) {
            val currentDemand = demand.get()
            if (currentDemand == Long.MAX_VALUE || currentDemand > 0L) return
            log.subscription.backpressure(name)
            val result = signal.receiveCatching()
            if (result.isClosed) return
        }
    }
}

internal class CompletionSubscription(
    private val subscriber: Subscriber<in Nothing>,
    private val source: suspend () -> Unit,
) : Subscription {

    private val log  = Logging.of<CompletionSubscription>()
    private val name = subscriber.javaClass.simpleName.ifEmpty { "Subscriber" }

    private val terminated = AtomicBoolean(false)
    private val producer   = AtomicReference(noopJob)

    private fun start() {
        log.stream.subscribing(name)
        val producerJob = sharedScope.launch {
            try {
                source()
                if (terminated.compareAndSet(false, true)) {
                    log.stream.completed(name)
                    subscriber.onComplete()
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (terminated.compareAndSet(false, true)) {
                    log.stream.error(name, e)
                    subscriber.onError(e)
                }
            }
        }
        if (!producer.compareAndSet(noopJob, producerJob)) producerJob.cancel()
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                producer.get().cancel()
                subscriber.onError(InvalidDemandException(n))
            }
            return
        }
        log.subscription.requested(name, n)
        start()
    }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            log.stream.cancelled(name)
            producer.get().cancel()
        }
    }
}
