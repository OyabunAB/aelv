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

import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
 * Base type for all aelv publishers: [Many], [One], [Maybe], and [None].
 *
 * [Observable] is part of the public API — it appears as a parameter type in [Verify.that]
 * and extension functions such as [discard] and [thenReturn]. Use it when writing code that
 * accepts any aelv publisher type.  It cannot be extended outside the module.
 *
 * Stores the computation as a [Step] ADT node and executes it through the heap-allocated
 * trampoline interpreter, giving all four types O(1) stack depth for arbitrary operator chains.
 *
 * [Self] is the concrete subtype. [wrap] constructs a new [Self] from a [Step.Suspend] node,
 * which is the escape hatch for operators that cannot be expressed as structural [Step] nodes.
 */
abstract class Observable<T : Any, Self : Observable<T, Self>> : Source<T> {

    internal abstract val step: Step<T>

    internal open suspend fun source(
        onNext:     OnNext<T>,
        onComplete: OnComplete,
        onError:    OnError,
    ) {
        when (val result = interpret(step, Frame.Collect(onNext))) {
            // Note: Step.Suspend sources that exit early due to downstream cancel return Success(true),
            // causing onComplete() to fire after a cancel — a structural RS §1.7 violation.
            // In practice this is harmless: StreamSubscription guards with terminated.compareAndSet,
            // and operators that use source() directly (zip, combineLatest) never observe the signal
            // because their channels are already closed or cancelled at that point.
            is Success -> if (result.value) onComplete()
            is Failure -> onError(result.value)
        }
    }

    internal abstract fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit,
    ): Self

    internal open suspend fun collect(
        action: OnNext<T>,
    ): Either<Exception, Unit> = when (val result = interpret(step, Frame.Collect(action))) {
        is Success -> Unit.right()
        is Failure -> result.value.left()
    }

    fun doOnNext(action: suspend (T) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            { value -> guardedSideEffectSuspend("doOnNext", log) { action(value) }; onNext(value) },
            onComplete,
            onError,
        )
    }

    fun doOnComplete(action: suspend () -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            { guardedSideEffectSuspend("doOnComplete", log) { action() }; onComplete() },
            onError,
        )
    }

    fun doOnError(action: suspend (Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        source(
            onNext,
            onComplete,
            { issue -> guardedSideEffectSuspend("doOnError", log) { action(issue) }; onError(issue) },
        )
    }

    fun doOnSubscribe(action: suspend () -> Unit): Self = wrap { onNext, onComplete, onError ->
        guardedSideEffectSuspend("doOnSubscribe", log) { action() }
        source(onNext, onComplete, onError)
    }

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

    fun recover(fallback: suspend (Exception) -> Self): Self = wrap { onNext, onComplete, onError ->
        source(onNext, onComplete, { issue -> fallback(issue).source(onNext, onComplete, onError) })
    }

    fun retry(times: Long = Long.MAX_VALUE): Self = retry(Policy.retry().maxAttempts(times))

    fun retry(policy: Policy.Retry): Self = wrap { onNext, onComplete, onError ->
        tailrec suspend fun attempt(attempts: Long) {
            var caught: Exception? = null
            source(onNext, { }, { e -> caught = e })
            val error = caught
            when {
                error == null                  -> onComplete()
                !policy.filter(error)          -> onError(error)
                attempts >= policy.maxAttempts -> { log.operator.retryExhausted("retry", error); onError(error) }
                else -> {
                    log.operator.retrying("retry", attempts, error)
                    val backoffDelay = policy.backoff.delayFor(attempts)
                    if (backoffDelay.isPositive()) delay(backoffDelay)
                    attempt(attempts + 1)
                }
            }
        }
        attempt(0L)
    }

    /**
     * Invokes [action] each time the source signals an error that will trigger a retry.
     * Must be placed **before** [retry] in the operator chain — if placed after, [retry]
     * intercepts the error internally and this operator's [onError] callback is never reached.
     *
     * @param action receives the zero-based attempt number and the error that triggered the retry.
     */
    fun doOnRetry(action: suspend (attempt: Long, cause: Exception) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var attempt = 0L
        source(
            onNext,
            onComplete,
            { cause -> guardedSideEffectSuspend("doOnRetry", log) { action(attempt++, cause) }; onError(cause) },
        )
    }

    /**
     * Invokes [action] once when the source successfully emits a value or completes after
     * at least one error — i.e., when recovery has occurred.  Fires on the first [onNext]
     * or [onComplete] signal that follows an [onError].
     *
     * Must be placed **before** [retry] in the operator chain so it observes the error signals.
     *
     * @param action receives the total number of errors that preceded this recovery.
     */
    fun doOnRecover(action: suspend (retries: Long) -> Unit): Self = wrap { onNext, onComplete, onError ->
        var retries  = 0L
        var recovered = false
        source(
            { value ->
                if (recovered) { recovered = false; guardedSideEffectSuspend("doOnRecover", log) { action(retries) } }
                onNext(value)
            },
            {
                if (recovered) { recovered = false; guardedSideEffectSuspend("doOnRecover", log) { action(retries) } }
                onComplete()
            },
            { cause -> retries++; recovered = true; onError(cause) },
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

    fun delayElement(delay: Duration): Self = wrap { onNext, onComplete, onError ->
        source(
            { value -> kotlinx.coroutines.delay(delay); onNext(value) },
            onComplete,
            onError,
        )
    }

    /**
     * Signals [ExceededTimeoutException] if no signal arrives from this source within [duration].
     *
     * The timeout window covers only the time upstream takes to emit each signal — downstream
     * processing is excluded. Resets on each received item, so this is a per-item deadline.
     */
    fun timeout(duration: Duration): Self = wrap { onNext, onComplete, onError ->
        coroutineScope {
            val inbox = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
            val producer = launch {
                source(
                    { value -> inbox.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                    { inbox.send(Signal.Upstream.Complete) },
                    { e -> inbox.send(Signal.Upstream.Error(e)) },
                )
            }
            var done = false
            while (!done) {
                when (val result = Either.catching(duration) { inbox.receive() }) {
                    is Failure -> { producer.cancel(); onError(result.value); done = true }
                    is Success -> when (val signal = result.value) {
                        is Signal.Upstream.Next     -> if (onNext(signal.value) == Signal.Downstream.Cancel) { producer.cancel(); done = true }
                        is Signal.Upstream.Complete -> { onComplete(); done = true }
                        is Signal.Upstream.Error    -> { onError(signal.cause); done = true }
                    }
                }
            }
        }
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

    open fun toMany(): Many<T> = Many.fromStep(step)

    open fun toMaybe(): Maybe<T> = Maybe.fromStep(step)

    fun <R : Any> thenReturn(value: R): One<R> = discard().andThen { One.single(value) }

    companion object {
        internal val log = Logging.of<Observable<*, *>>()
    }
}

private suspend fun guardedSideEffectSuspend(name: String, log: Log, action: suspend () -> Unit) {
    runCatching { action() }.onFailure { e -> log.operator.sideEffectThrew(name, e) }
}
