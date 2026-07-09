package se.oyabun.aelv

import kotlinx.coroutines.channels.Channel
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

/**
 * A hot multicast push source.
 *
 * Push items with [emit], which suspends until every current subscriber has received the item
 * (backpressure).  Signal terminal state with [complete] or [error] — both are non-suspend
 * fire-and-forget calls.
 *
 * Obtain subscribable views via [asMany] (0–N items) or [asOne] (first item only).
 *
 * Late subscribers — those that subscribe after [complete] or [error] has been called — receive
 * the terminal signal immediately without missing any items.
 *
 * Usage:
 * ```kotlin
 * val sink = Sink<Int>()
 * val many = sink.asMany()
 * // in a coroutine:
 * sink.emit(1)
 * sink.emit(2)
 * sink.complete()
 * ```
 */
class Sink<T : Any> {

    // Non-null once complete() or error() has been called.  First write wins.
    private val terminal = AtomicReference<Signal.Upstream<T>?>(null)

    // Subscriber channels.  CopyOnWriteArrayList so terminate() can iterate without
    // blocking emit(), and add/remove are safe without a shared lock.
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    /**
     * Emits [value] to all current subscribers.
     *
     * Suspends per subscriber channel until the item has been accepted — the caller does not
     * need any retry logic.  If the sink is already terminated, the item is silently dropped.
     */
    suspend fun emit(value: T) {
        if (terminal.get() != null) return
        for (ch in subscribers) {
            ch.send(Signal.Upstream.Next(value))
        }
    }

    /**
     * Signals successful completion to all current and future subscribers.
     * Idempotent — the first call wins; subsequent calls are ignored.
     */
    fun complete() = terminate(Signal.Upstream.Complete)

    /**
     * Signals an error to all current and future subscribers.
     * Idempotent — the first call wins; subsequent calls are ignored.
     */
    fun error(cause: AelvException) = terminate(Signal.Upstream.Error(cause))

    private fun terminate(signal: Signal.Upstream<T>) {
        if (!terminal.compareAndSet(null, signal)) return  // already terminated
        for (ch in subscribers) {
            ch.trySend(signal)
            ch.close()
        }
    }

    /**
     * Returns a [Many] view of this sink.  Each subscription receives items emitted after
     * subscription time plus the terminal signal.
     */
    fun asMany(): Many<T> = Many.generate { emit ->
        val ch = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
        // Register before checking terminal to avoid a race where terminate() runs between
        // the two steps and we miss the terminal signal.
        subscribers.add(ch)
        val replay = terminal.get()
        if (replay != null) {
            // Sink terminated before or during our registration — deliver terminal and exit.
            subscribers.remove(ch)
            emit(replay)
            return@generate
        }
        try {
            for (signal in ch) {
                if (emit(signal) == Signal.Downstream.Cancel) break
            }
        } finally {
            subscribers.remove(ch)
        }
    }

    /**
     * Returns a [One] view of this sink.  Completes after the first item, or forwards the
     * terminal signal if the sink completes/errors before emitting any item.
     */
    fun asOne(): One<T> = Many.generate<T> { emit ->
        val ch = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
        subscribers.add(ch)
        val replay = terminal.get()
        if (replay != null) {
            subscribers.remove(ch)
            emit(replay)
            return@generate
        }
        try {
            for (signal in ch) {
                val downstream = emit(signal)
                // Stop after first Next — One<T> semantics.
                if (signal is Signal.Upstream.Next || downstream == Signal.Downstream.Cancel) break
            }
        } finally {
            subscribers.remove(ch)
        }
    }.let { many ->
        One.generate { emit -> many.source { emit(it) } }
    }
}
