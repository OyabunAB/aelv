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

internal class StreamSubscription<T : Any>(
    private val subscriber: Subscriber<in T>,
    private val scope: CoroutineScope,
    private val source: suspend (emit: suspend (T) -> Unit) -> Unit,
) : Subscription {

    private val demand = AtomicLong(0L)
    private val signal = Channel<Unit>(Channel.UNLIMITED)
    private val terminated = AtomicBoolean(false)
    private val pending = Channel<T>(Channel.BUFFERED)
    private val started = AtomicBoolean(false)

    private lateinit var producer: Job
    private lateinit var consumer: Job

    private fun start() {
        if (!started.compareAndSet(false, true)) return

        producer = scope.launch {
            try {
                source { item ->
                    awaitDemand()
                    pending.send(item)
                }
                pending.close()
            } catch (e: CancellationException) {
                pending.close(e)
                throw e
            } catch (e: Throwable) {
                pending.close(e)
            }
        }

        consumer = scope.launch {
            try {
                for (item in pending) {
                    demand.decrementAndGet()
                    subscriber.onNext(item)
                }
                val cause = pending.closedCause()
                if (terminated.compareAndSet(false, true)) {
                    when {
                        cause == null -> subscriber.onComplete()
                        cause is CancellationException -> Unit
                        else -> subscriber.onError(cause)
                    }
                }
            } catch (e: CancellationException) {
                // downstream cancelled
            } catch (e: Throwable) {
                if (terminated.compareAndSet(false, true)) subscriber.onError(e)
            }
        }
    }

    override fun request(n: Long) {
        if (n <= 0L) {
            if (terminated.compareAndSet(false, true)) {
                cancel()
                subscriber.onError(InvalidDemandException(n))
            }
            return
        }
        demand.getAndUpdate { current ->
            if (current >= Long.MAX_VALUE / 2) Long.MAX_VALUE else current + n
        }
        repeat(n.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()) { signal.trySend(Unit) }
        start()
    }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            if (started.get()) {
                producer.cancel()
                consumer.cancel()
            }
            pending.close(CancellationException("subscription cancelled"))
            signal.close()
        }
    }

    private suspend fun awaitDemand() {
        while (demand.get() <= 0L && !terminated.get()) {
            signal.receive()
        }
    }
}

internal class CompletionSubscription(
    private val subscriber: Subscriber<in Nothing>,
    private val scope: CoroutineScope,
    private val source: suspend () -> Unit,
) : Subscription {

    private val terminated = AtomicBoolean(false)
    private val started = AtomicBoolean(false)
    private lateinit var producer: Job

    private fun start() {
        if (!started.compareAndSet(false, true)) return
        producer = scope.launch {
            try {
                source()
                if (terminated.compareAndSet(false, true)) subscriber.onComplete()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (terminated.compareAndSet(false, true)) subscriber.onError(e)
            }
        }
    }

    override fun request(n: Long) { start() }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            if (started.get()) producer.cancel()
        }
    }
}

private fun <T> Channel<T>.closedCause(): Throwable? = tryReceive().exceptionOrNull()
