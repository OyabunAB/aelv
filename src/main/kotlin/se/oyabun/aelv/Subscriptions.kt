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

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

private sealed interface ProducerState {
    data object Idle                : ProducerState
    data class  Running(val job: Job) : ProducerState
    fun cancel() = if (this is Running) job.cancel() else Unit
}

internal sealed interface SubscriptionState {
    data object Unbound                      : SubscriptionState
    data class  Bound(val subscription: Subscription) : SubscriptionState
}

/** Sentinel demand value meaning the subscriber accepts items at any rate — RS §3.17. */
internal const val UNBOUNDED = Long.MAX_VALUE

private val sharedScope = CoroutineScope(Dispatchers.cpu + SupervisorJob())

private val noopJob: Job = Job().also { it.cancel() }

internal fun <S> S.deliverSubscription(
    subscriber: org.reactivestreams.Subscriber<*>,
    cancel: () -> Unit,
    onSubscribeComplete: () -> Unit,
) where S : Subscription {
    Either.catchingStrict { subscriber.onSubscribe(this) }
        .fold(onLeft = { cancel() }, onRight = { onSubscribeComplete() })
}

internal class StreamSubscription<T : Any>(
    private val subscriber: Subscriber<in T>,
    private val source: suspend (
        onNext:     OnNext<T>,
        onComplete: OnComplete,
        onError:    OnError,
    ) -> Unit,
) : Subscription {

    private val log  = Logging.of<StreamSubscription<*>>()
    private val name = subscriber.javaClass.simpleName.ifEmpty { "Subscriber" }

    private val demand     = AtomicLong(0L)
    private val signal     by lazy { Channel<Unit>(Channel.UNLIMITED) }  // only allocated for bounded demand
    private val terminated = AtomicBoolean(false)
    private val started    = AtomicBoolean(false)
    private val producer   = AtomicReference<ProducerState>(ProducerState.Idle)

    internal fun onSubscribeComplete() {
        // onSubscribe has returned — safe to start the producer now (RS §1.3: serial signals)
        if (started.compareAndSet(false, true)) start()
    }

    private fun start() {
        log.stream.subscribing(name)
        val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
            if (terminated.compareAndSet(false, true)) {
                if (throwable is Exception) {
                    log.stream.error(name, throwable)
                    subscriber.onError(throwable)
                } else {
                    val exception = RuntimeException(throwable)
                    log.stream.unexpectedThrowable(name, throwable)
                    subscriber.onError(exception)
                }
            }
            shutdown()
        }
        val onComplete: OnComplete = {
            if (terminated.compareAndSet(false, true)) {
                log.stream.completed(name)
                subscriber.onComplete()
            }
            shutdown()
        }
        val onError: OnError = { cause ->
            if (terminated.compareAndSet(false, true)) {
                log.stream.error(name, cause)
                subscriber.onError(cause)
            }
            shutdown()
        }
        val producerJob = sharedScope.launch(exceptionHandler) {
            if (demand.get() == UNBOUNDED) {
                // Fast path: unbounded demand — skip awaitDemand() and CAS per item
                source(
                    { value ->
                        if (terminated.get()) return@source Signal.Downstream.Cancel
                        subscriber.onNext(value)
                        if (terminated.get()) Signal.Downstream.Cancel else Signal.Downstream.Request
                    },
                    onComplete,
                    onError,
                )
            } else {
                source(
                    { value ->
                        awaitDemand()
                        if (terminated.get()) return@source Signal.Downstream.Cancel
                        demand.updateAndGet { d -> if (d == UNBOUNDED) d else d - 1 }
                        subscriber.onNext(value)
                        if (terminated.get()) Signal.Downstream.Cancel else Signal.Downstream.Request
                    },
                    onComplete,
                    onError,
                )
            }
        }
        producer.set(ProducerState.Running(producerJob))
        if (terminated.get()) producerJob.cancel()
    }

    private fun shutdown() {
        if (demand.get() < UNBOUNDED) signal.close()  // only close if it was ever allocated
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
            // RS §3.17: cumulative demand overflow must be treated as unbounded.
            // Check headroom before adding to avoid signed overflow.
            if (UNBOUNDED - current < n) UNBOUNDED else current + n
        }
        if (demand.get() < UNBOUNDED) signal.trySend(Unit)  // only meaningful for bounded demand
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
            if (currentDemand == UNBOUNDED || currentDemand > 0L) return
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

    private val terminated    = AtomicBoolean(false)
    private val started       = AtomicBoolean(false)
    private val producer      = AtomicReference<ProducerState>(ProducerState.Idle)

    internal fun onSubscribeComplete() {
        if (started.compareAndSet(false, true)) start()
    }

    private fun start() {
        log.stream.subscribing(name)
        val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
            if (terminated.compareAndSet(false, true)) {
                if (throwable is Exception) {
                    log.stream.error(name, throwable)
                    subscriber.onError(throwable)
                } else {
                    val exception = RuntimeException(throwable)
                    log.stream.unexpectedThrowable(name, throwable)
                    subscriber.onError(exception)
                }
            }
        }
        val producerJob = sharedScope.launch(exceptionHandler) {
            source()
            if (terminated.compareAndSet(false, true)) {
                log.stream.completed(name)
                subscriber.onComplete()
            }
        }
        if (!producer.compareAndSet(ProducerState.Idle, ProducerState.Running(producerJob))) producerJob.cancel()
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
    }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            log.stream.cancelled(name)
            producer.get().cancel()
        }
    }
}
