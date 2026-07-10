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

private val dispatcher = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()) { r ->
    Thread(r, "aelv-worker").also { it.isDaemon = true }
}.asCoroutineDispatcher()

private val sharedScope = CoroutineScope(dispatcher + SupervisorJob())

internal class StreamSubscription<T : Any>(
    private val subscriber: Subscriber<in T>,
    private val source: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (AelvException) -> Unit,
    ) -> Unit,
) : Subscription {

    private val log  = Logging.of<StreamSubscription<*>>()
    private val name = subscriber::class.simpleName ?: "Subscriber"

    private val demand     = AtomicLong(0L)
    private val signal     = Channel<Unit>(Channel.UNLIMITED)
    private val terminated = AtomicBoolean(false)
    private val started    = AtomicBoolean(false)
    private val producer   = AtomicReference<Job?>(null)

    private fun start() {
        log.stream.subscribing(name)
        val job = sharedScope.launch {
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
            } catch (e: Throwable) {
                if (terminated.compareAndSet(false, true)) {
                    log.stream.error(name, e)
                    subscriber.onError(e)
                }
                shutdown()
            }
        }
        producer.set(job)
        if (terminated.get()) job.cancel()
    }

    private fun shutdown() {
        signal.close()
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                producer.get()?.cancel()
                signal.close()
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
            producer.get()?.cancel()
            shutdown()
        }
    }

    private suspend fun awaitDemand() {
        while (!terminated.get()) {
            val d = demand.get()
            if (d == Long.MAX_VALUE || d > 0L) return
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
    private val name = subscriber::class.simpleName ?: "Subscriber"

    private val terminated = AtomicBoolean(false)
    private val producer   = AtomicReference<Job?>(null)

    private fun start() {
        log.stream.subscribing(name)
        val job = sharedScope.launch {
            try {
                source()
                if (terminated.compareAndSet(false, true)) {
                    log.stream.completed(name)
                    subscriber.onComplete()
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (terminated.compareAndSet(false, true)) {
                    log.stream.error(name, e)
                    subscriber.onError(e)
                }
            }
        }
        if (!producer.compareAndSet(null, job)) job.cancel()
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                producer.get()?.cancel()
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
            producer.get()?.cancel()
        }
    }
}
