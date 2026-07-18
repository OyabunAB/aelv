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
 * Internal base for [Many], [One], [Maybe], and [None].
 *
 * Stores the computation as a [Step] ADT node and executes it through the heap-allocated
 * trampoline interpreter, giving all four types O(1) stack depth for arbitrary operator chains.
 *
 * [Self] is the concrete subtype. [wrap] constructs a new [Self] from a [Step.Suspend] node,
 * which is the escape hatch for operators that cannot be expressed as structural [Step] nodes.
 */
internal abstract class Observable<T : Any, Self : Observable<T, Self>> : Source<T> {

    internal abstract val step: Step<T>

    internal open suspend fun source(
        onNext:     suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError:    suspend (Exception) -> Unit,
    ) {
        when (val result = interpret(step, Frame.Collect(onNext))) {
            is Success -> if (result.value) onComplete()
            is Failure -> onError(result.value)
        }
    }

    internal abstract fun wrap(
        block: suspend (
            onNext:     suspend (T) -> Signal.Downstream,
            onComplete: suspend () -> Unit,
            onError:    suspend (Exception) -> Unit,
        ) -> Unit,
    ): Self

    internal open suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Exception, Unit> = when (val result = interpret(step, Frame.Collect(action))) {
        is Success -> Unit.right()
        is Failure -> result.value.left()
    }

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

    fun recover(fallback: (Exception) -> Self): Self = wrap { onNext, onComplete, onError ->
        source(onNext, onComplete, { issue -> fallback(issue).source(onNext, onComplete, onError) })
    }

    @LowPriorityInOverloadResolution
    fun recover(fallback: suspend (Exception) -> Self): Self = wrap { onNext, onComplete, onError ->
        source(onNext, onComplete, { issue -> fallback(issue).source(onNext, onComplete, onError) })
    }

    fun retry(times: Long = Long.MAX_VALUE): Self = retry(Policy.retry().maxAttempts(times))

    fun retry(policy: Policy.Retry): Self = wrap { onNext, onComplete, onError ->
        val result = retryLoop(policy, log) { Either.catching { source(onNext, onComplete, onError) } }
        if (result is Failure) onError(result.value) else onComplete()
    }

    fun doOnRetry(action: (attempt: Long, cause: Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var attempt = 0L
        source(
            onNext,
            onComplete,
            { cause -> guardedSideEffect("doOnRetry", log) { action(attempt++, cause) }; onError(cause) },
        )
    }

    @LowPriorityInOverloadResolution
    fun doOnRetry(action: suspend (attempt: Long, cause: Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var attempt = 0L
        source(
            onNext,
            onComplete,
            { cause -> guardedSideEffectSuspend("doOnRetry", log) { action(attempt++, cause) }; onError(cause) },
        )
    }

    fun doOnRecover(action: (retries: Long) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var retries = 0L
        source(
            { value -> if (retries > 0) guardedSideEffect("doOnRecover", log) { action(retries) }; onNext(value) },
            { if (retries > 0) guardedSideEffect("doOnRecover", log) { action(retries) }; onComplete() },
            { cause -> retries++; onError(cause) },
        )
    }

    @LowPriorityInOverloadResolution
    fun doOnRecover(action: suspend (retries: Long) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var retries = 0L
        source(
            { value -> if (retries > 0) guardedSideEffectSuspend("doOnRecover", log) { action(retries) }; onNext(value) },
            { if (retries > 0) guardedSideEffectSuspend("doOnRecover", log) { action(retries) }; onComplete() },
            { cause -> retries++; onError(cause) },
        )
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

    fun discard(): None<T> = None.defer { collect { Signal.Downstream.Request }.let { if (it is Failure) throw it.value } }

    fun <R : Any> thenReturn(value: R): One<R> = discard().then { One.single(value) }

    companion object {
        internal val log = Logging.of<Observable<*, *>>()
    }
}
