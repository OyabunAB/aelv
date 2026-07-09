package se.oyabun.aelv

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

internal class StreamSubscription<T : Any>(
    private val subscriber: Subscriber<in T>,
    private val scope: CoroutineScope,
    private val source: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
) : Subscription {

    private val log = Logging.of<StreamSubscription<*>>()
    private val name = subscriber::class.simpleName ?: "Subscriber"

    private val demand = AtomicLong(0L)
    private val signal = Channel<Unit>(Channel.UNLIMITED)
    private val terminated = AtomicBoolean(false)
    private val started = AtomicBoolean(false)
    private val producer = AtomicReference<Job?>(null)

    private fun start() {
        log.stream.subscribing(name)
        val job = scope.launch {
            try {
                source { upstream ->
                    when (upstream) {
                        is Signal.Upstream.Next -> {
                            awaitDemand()
                            if (terminated.get()) return@source Signal.Downstream.Cancel
                            // Preserve Long.MAX_VALUE unbounded sentinel — do not decrement.
                            demand.updateAndGet { d -> if (d == Long.MAX_VALUE) d else d - 1 }
                            subscriber.onNext(upstream.value)
                            if (terminated.get()) Signal.Downstream.Cancel
                            else Signal.Downstream.Request(1)
                        }
                        is Signal.Upstream.Complete -> {
                            if (terminated.compareAndSet(false, true)) {
                                log.stream.completed(name)
                                subscriber.onComplete()
                            }
                            Signal.Downstream.Cancel
                        }
                        is Signal.Upstream.Error -> {
                            if (terminated.compareAndSet(false, true)) {
                                log.stream.error(name, upstream.cause)
                                subscriber.onError(upstream.cause)
                            }
                            Signal.Downstream.Cancel
                        }
                    }
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
        // Store the job first, then check terminated so cancel() always sees it.
        producer.set(job)
        if (terminated.get()) job.cancel()
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                producer.get()?.cancel()
                signal.close()
                subscriber.onError(InvalidDemandException(n))
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
            signal.close()
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
    private val scope: CoroutineScope,
    private val source: suspend () -> Unit,
) : Subscription {

    private val log = Logging.of<CompletionSubscription>()
    private val name = subscriber::class.simpleName ?: "Subscriber"

    private val terminated = AtomicBoolean(false)
    private val producer = AtomicReference<Job?>(null)

    private fun start() {
        log.stream.subscribing(name)
        val job = scope.launch {
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
