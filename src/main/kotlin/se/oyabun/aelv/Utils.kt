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
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.Job

internal object Unset

private typealias Cached<T> = Either<Unset, T>

/**
 * A handle returned by [subscribe] and [drain] that allows cancelling an active subscription.
 */
interface Disposable {
    /** Cancels the subscription, stopping item delivery. */
    fun cancel()
}

sealed interface TimerState {
    data object Idle                : TimerState
    data class  Running(val job: Job) : TimerState
    fun cancel() = if (this is Running) job.cancel() else Unit
}

private sealed interface BufferEvent<out T : Any> {
    data object TimerFlush                                    : BufferEvent<Nothing>
    data class  SourceSignal<out T : Any>(val signal: Signal.Upstream<T>) : BufferEvent<T>
}

private sealed interface Tagged<out A : Any, out B : Any>
private data class FromA<out A : Any>(val value: A) : Tagged<A, Nothing>
private data class FromB<out B : Any>(val value: B) : Tagged<Nothing, B>

internal fun AtomicReference<Any>.isSet(): Boolean = get() !== Unset

internal fun Any.isError(): Boolean = this is Exception
internal fun Any.asError(): Exception = this as Exception

internal fun rethrow(issue: Exception): Nothing = throw issue

fun Exception.leftUnlessCancelled(): Either<Exception, Nothing> =
    if (this is CancellationException) throw this else this.left()

internal suspend fun <T> Flow<T>.collectCancelling(block: suspend (T) -> Boolean) {
    var selfCancelled = false
    try {
        coroutineScope {
            collect {
                if (!block(it)) {
                    selfCancelled = true
                    cancel()
                }
            }
        }
    } catch (e: CancellationException) {
        if (!selfCancelled) throw e
    }
}

internal suspend inline fun <C : AutoCloseable, V> C.using(block: suspend (C) -> V): V =
    try { block(this) } finally { close() }
