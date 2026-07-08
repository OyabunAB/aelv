package se.oyabun.aelv

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    private val started = AtomicBoolean(false)
    private val emitMutex = Mutex()

    private lateinit var producer: Job

    private fun start() {
        if (!started.compareAndSet(false, true)) return
        producer = scope.launch {
            try {
                source { item ->
                    awaitDemand()
                    if (terminated.get()) return@source
                    emitMutex.withLock {
                        if (!terminated.get()) {
                            demand.decrementAndGet()
                            subscriber.onNext(item)
                        }
                    }
                }
                if (terminated.compareAndSet(false, true)) subscriber.onComplete()
            } catch (e: CancellationException) {
                throw e
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
        demand.updateAndGet { current ->
            if (current >= Long.MAX_VALUE / 2) Long.MAX_VALUE else current + n
        }
        signal.trySend(Unit)
        start()
    }

    override fun cancel() {
        if (terminated.compareAndSet(false, true)) {
            if (started.get()) producer.cancel()
            signal.close()
        }
    }

    private suspend fun awaitDemand() {
        while (!terminated.get()) {
            val d = demand.get()
            if (d == Long.MAX_VALUE || d > 0L) return
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
