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
@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
@file:OptIn(kotlin.experimental.ExperimentalTypeInference::class)
package se.oyabun.aelv

import kotlin.internal.LowPriorityInOverloadResolution
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import org.reactivestreams.Publisher

/**
 * Common sealed marker for all aelv publisher types: [Many], [One], [Maybe], and [None].
 *
 * Carries no operator methods. Being sealed prevents external implementations and enables
 * exhaustive `when` matching over the four types.
 */
sealed interface Source<out T : Any>

/**
 * Internal base for [Many], [One], and [Maybe].
 *
 * Provides shared implementations of operators that are identical across all three types.
 * [Self] is the concrete subtype; [wrap] constructs a new [Self] from a triple-callback source.
 *
 * [None] is not an [Observable] — its source carries no items.
 */
internal abstract class Observable<T : Any, Self : Observable<T, Self>> : Source<T> {

    internal abstract suspend fun source(
        onNext:     suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError:    suspend (Exception) -> Unit,
    )

    internal abstract fun wrap(
        block: suspend (
            onNext:     suspend (T) -> Signal.Downstream,
            onComplete: suspend () -> Unit,
            onError:    suspend (Exception) -> Unit,
        ) -> Unit,
    ): Self

    fun doOnNext(action: (T) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            { value -> guardedSideEffectSuspend("doOnNext", log) { action(value) }; onNext(value) },
            onComplete,
            onError,
        )
    }

    @LowPriorityInOverloadResolution
    fun doOnNext(action: suspend (T) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            { value -> guardedSideEffectSuspend("doOnNext", log) { action(value) }; onNext(value) },
            onComplete,
            onError,
        )
    }

    fun doOnComplete(action: () -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            { guardedSideEffectSuspend("doOnComplete", log) { action() }; onComplete() },
            onError,
        )
    }

    @LowPriorityInOverloadResolution
    fun doOnComplete(action: suspend () -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            { guardedSideEffectSuspend("doOnComplete", log) { action() }; onComplete() },
            onError,
        )
    }

    fun doOnError(action: (Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            onComplete,
            { issue -> guardedSideEffectSuspend("doOnError", log) { action(issue) }; onError(issue) },
        )
    }

    @LowPriorityInOverloadResolution
    fun doOnError(action: suspend (Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            onComplete,
            { issue -> guardedSideEffectSuspend("doOnError", log) { action(issue) }; onError(issue) },
        )
    }

    fun doOnSubscribe(action: () -> Unit): Self = wrap { onNext, onComplete, onError ->
        guardedSideEffectSuspend("doOnSubscribe", log) { action() }
        source(onNext, onComplete, onError)
    }

    @LowPriorityInOverloadResolution
    fun doOnSubscribe(action: suspend () -> Unit): Self = wrap { onNext, onComplete, onError ->
        guardedSideEffectSuspend("doOnSubscribe", log) { action() }
        source(onNext, onComplete, onError)
    }

    fun doFinally(action: (Signal.Terminal) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            { value ->
                val downstream = onNext(value)
                if (downstream == Signal.Downstream.Cancel) guardedSideEffectSuspend("doFinally", log) { action(Signal.Downstream.Cancel) }
                downstream
            },
            { guardedSideEffectSuspend("doFinally", log) { action(Signal.Upstream.Complete) }; onComplete() },
            { issue -> guardedSideEffectSuspend("doFinally", log) { action(Signal.Upstream.Error(issue)) }; onError(issue) },
        )
    }

    @LowPriorityInOverloadResolution
    fun doFinally(action: suspend (Signal.Terminal) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            { value ->
                val downstream = onNext(value)
                if (downstream == Signal.Downstream.Cancel) guardedSideEffectSuspend("doFinally", log) { action(Signal.Downstream.Cancel) }
                downstream
            },
            { guardedSideEffectSuspend("doFinally", log) { action(Signal.Upstream.Complete) }; onComplete() },
            { issue -> guardedSideEffectSuspend("doFinally", log) { action(Signal.Upstream.Error(issue)) }; onError(issue) },
        )
    }

    fun retry(times: Long = Long.MAX_VALUE): Self = retry(Policy.retry().maxAttempts(times))

    fun retry(policy: Policy.Retry): Self = wrap { onNext, onComplete, onError ->
        val result = retryLoop(policy, log) { Either.catching { source(onNext, onComplete, onError) } }
        if (result is Failure) onError(result.value) else onComplete()
    }

    fun publishOn(context: CoroutineContext): Self = wrap { onNext, onComplete, onError ->
        source(
            { value -> withContext(currentCoroutineContext() + context) { onNext(value) } },
            { withContext(currentCoroutineContext() + context) { onComplete() } },
            { issue -> withContext(currentCoroutineContext() + context) { onError(issue) } },
        )
    }

    fun subscribeOn(context: CoroutineContext): Self = wrap { onNext, onComplete, onError ->
        withContext(currentCoroutineContext() + context) {
            source(onNext, onComplete, onError)
        }
    }

    fun delaySubscription(delay: Duration): Self = wrap { onNext, onComplete, onError ->
        kotlinx.coroutines.delay(delay)
        source(onNext, onComplete, onError)
    }

    fun delaySubscription(trigger: Publisher<*>): Self = wrap { onNext, onComplete, onError ->
        var triggerFailed = false
        Many.from(trigger).source(
            { Signal.Downstream.Cancel },
            { },
            { issue -> triggerFailed = true; onError(issue) },
        )
        if (!triggerFailed) source(onNext, onComplete, onError)
    }

    companion object {
        internal val log = Logging.of<Observable<*, *>>()
    }
}
