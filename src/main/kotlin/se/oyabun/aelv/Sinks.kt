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
import kotlinx.coroutines.channels.ClosedSendChannelException
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
    private val terminal    = AtomicReference<Any>(Unset)
    private val history     = ArrayDeque<Signal.Upstream.Next<T>>()
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    suspend fun emit(value: T) {
        val signal = Signal.Upstream.Next(value)
        lock.withLock {
            if (terminal.get() !== Unset) return
            if (historySize > 0) {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
        }
        for (ch in subscribers) {
            try { ch.send(signal) } catch (_: ClosedSendChannelException) {}
        }
    }

    fun tryEmit(value: T): Boolean {
        if (terminal.get() !== Unset) return false
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
        if (!terminal.compareAndSet(Unset, signal)) return
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
        val t = terminal.get()
        if (t !== Unset) {
            subscribers.remove(ch)
            when (t) {
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(t.cause))
                else                        -> {}
            }
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

    fun asOne(): One<T> = One.generate { emit ->
        val ch = Channel<Signal.Upstream<T>>(bufferSize)
        val replay = lock.withLock {
            subscribers.add(ch)
            history.toList()
        }
        try {
            for (item in replay) {
                emit(item)
                emit(Signal.Upstream.Complete)
                return@generate
            }
            val t = terminal.get()
            if (t !== Unset) {
                when (t) {
                    is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                    is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(t.cause))
                    else                        -> {}
                }
                return@generate
            }
            for (signal in ch) {
                when {
                    signal is Signal.Upstream.Next -> { emit(signal); emit(Signal.Upstream.Complete); return@generate }
                    else -> { emit(signal); return@generate }
                }
            }
        } finally {
            subscribers.remove(ch)
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
