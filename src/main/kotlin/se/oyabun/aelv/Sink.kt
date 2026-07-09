package se.oyabun.aelv

import kotlinx.coroutines.channels.Channel
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A hot multicast push source.
 *
 * Obtain an instance via the factory functions:
 * - [Sink.broadcast] — no buffering; subscribers only receive items emitted after subscription
 * - [Sink.replay] — buffers all items; late subscribers receive the full history then live items
 * - [Sink.replayLast] — buffers the last [n] items; late subscribers receive recent history then live items
 *
 * Signal terminal state with [complete] or [error] — both are non-suspend fire-and-forget calls.
 * [emit] is also non-suspend — subscriber channels are unbounded so sends never block.
 * Obtain subscribable views via [asMany] or [asOne].
 */
class Sink<T : Any> private constructor(
    private val historySize: Int,  // Int.MAX_VALUE = all, 0 = none, n = last n
) {
    private val lock        = ReentrantLock()
    private val terminal    = AtomicReference<Signal.Upstream<T>?>(null)
    private val history     = ArrayDeque<Signal.Upstream.Next<T>>()
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    fun emit(value: T) {
        if (terminal.get() != null) return
        val signal = Signal.Upstream.Next(value)
        lock.withLock {
            if (historySize > 0) {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
            for (ch in subscribers) ch.trySend(signal)
        }
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
        val ch = Channel<Signal.Upstream<T>>(Channel.UNLIMITED)
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
        val ch = Channel<Signal.Upstream<T>>(Channel.UNLIMITED)
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
        One.generate { emit -> many.source { emit(it) } }
    }

    companion object {
        fun <T : Any> broadcast(): Sink<T> = Sink(historySize = 0)
        fun <T : Any> replay(): Sink<T>    = Sink(historySize = Int.MAX_VALUE)
        fun <T : Any> replayLast(n: Int): Sink<T> {
            require(n > 0) { "replayLast requires n > 0, got $n" }
            return Sink(historySize = n)
        }
    }
}
