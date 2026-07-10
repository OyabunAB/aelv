package se.oyabun.aelv

import kotlinx.coroutines.channels.Channel
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private const val DEFAULT_BUFFER = 256

/**
 * A hot multicast push source.
 *
 * Push items with [emit] (suspend — backs off when the slowest subscriber's channel is full)
 * or [tryEmit] (non-suspend — returns false if any subscriber channel is full).
 *
 * Signal terminal state with [complete] or [error].
 * Obtain subscribable views via [asMany] or [asOne].
 *
 * Obtain instances via:
 * - [Sinks.broadcast]
 * - [Sinks.replay]
 * - [Sinks.replayLast]
 */
sealed class Sink<T : Any>(
    private val historySize: Int,
    private val bufferSize: Int,
) {
    private val lock        = ReentrantLock()
    private val terminal    = AtomicReference<Signal.Upstream<T>?>(null)
    private val history     = ArrayDeque<Signal.Upstream.Next<T>>()
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    suspend fun emit(value: T) {
        if (terminal.get() != null) return
        val signal = Signal.Upstream.Next(value)
        lock.withLock {
            if (historySize > 0) {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
        }
        for (ch in subscribers) ch.send(signal)
    }

    fun tryEmit(value: T): Boolean {
        if (terminal.get() != null) return false
        val signal = Signal.Upstream.Next(value)
        lock.withLock {
            if (historySize > 0) {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
            for (ch in subscribers) {
                if (!ch.trySend(signal).isSuccess) return false
            }
        }
        return true
    }

    fun complete() = terminate(Signal.Upstream.Complete)

    fun error(cause: AelvException) = terminate(Signal.Upstream.Error(cause))

    private fun terminate(signal: Signal.Upstream<T>) {
        if (!terminal.compareAndSet(null, signal)) return
        for (ch in subscribers) {
            ch.trySend(signal)
            ch.close()
        }
    }

    fun asMany(): Many<T> = Many.generate { emit ->
        val ch = Channel<Signal.Upstream<T>>(bufferSize)
        val replay = lock.withLock {
            subscribers.add(ch)
            history.toList()
        }
        for (item in replay) {
            if (emit(item) == Signal.Downstream.Cancel) {
                subscribers.remove(ch)
                return@generate
            }
        }
        terminal.get()?.let { t ->
            subscribers.remove(ch)
            emit(t)
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

    fun asOne(): One<T> = Many.generate<T> { emit ->
        val ch = Channel<Signal.Upstream<T>>(bufferSize)
        val replay = lock.withLock {
            subscribers.add(ch)
            history.toList()
        }
        var done = false
        for (item in replay) {
            val downstream = emit(item)
            if (item is Signal.Upstream.Next || downstream == Signal.Downstream.Cancel) {
                done = true; break
            }
        }
        if (!done) {
            terminal.get()?.let { t ->
                subscribers.remove(ch)
                emit(t)
                return@generate
            }
            try {
                for (signal in ch) {
                    val downstream = emit(signal)
                    if (signal is Signal.Upstream.Next || downstream == Signal.Downstream.Cancel) break
                }
            } finally { }
        }
        subscribers.remove(ch)
    }.let { many ->
        One.generate { emit ->
            many.source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { e -> emit(Signal.Upstream.Error(e)) },
            )
        }
    }
}

/** Emits only to subscribers present at the time of emission; no history. */
class BroadcastSink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = 0,
    bufferSize  = bufferSize,
)

/** Buffers the full emission history; late subscribers receive all past items then live items. */
class ReplaySink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = Int.MAX_VALUE,
    bufferSize  = bufferSize,
)

/** Buffers the last [n] items; late subscribers receive recent history then live items. */
class ReplayLastSink<T : Any>(val n: Int, bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = n,
    bufferSize  = bufferSize,
) {
    init { require(n > 0) { "ReplayLastSink requires n > 0, got $n" } }
}

object Sinks {
    fun <T : Any> broadcast(bufferSize: Int = DEFAULT_BUFFER): BroadcastSink<T> =
        BroadcastSink(bufferSize)

    fun <T : Any> replay(bufferSize: Int = DEFAULT_BUFFER): ReplaySink<T> =
        ReplaySink(bufferSize)

    fun <T : Any> replayLast(n: Int, bufferSize: Int = DEFAULT_BUFFER): ReplayLastSink<T> =
        ReplayLastSink(n, bufferSize)
}
