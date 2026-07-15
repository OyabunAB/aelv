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

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.reactivestreams.Publisher
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds


/**
 * Reactive Streams publisher verifier for aelv.
 *
 * Usage:
 * ```kotlin
 * Verify.that(publisher)
 *     .emitsNext(1, 2, 3)
 *     .completesNormally()
 * ```
 */
class Verify<T : Any> private constructor(
    private val publisher: Publisher<T>,
    private val timeout:   Duration = 5.seconds,
    private val context:   CoroutineContext = EmptyCoroutineContext,
) {

    private sealed interface Step<T> {
        data class EmitsNext<T>(val values: List<T>) : Step<T>
        data class EmitsCount<T>(val count: Long) : Step<T>
        data class MatchesNext<T>(val assertion: (T) -> Unit) : Step<T>
        data class Runs<T>(val action: () -> Unit) : Step<T>
        class ThenCancels<T> : Step<T>
        class IsSubscribed<T> : Step<T>
    }

    private sealed interface Terminal
    private data object Complete : Terminal
    private data object Cancel   : Terminal
    private data class  Errored(val cause: Exception) : Terminal

    private val steps = mutableListOf<Step<T>>()

    fun isSubscribed(): Verify<T>                        = apply { steps.add(Step.IsSubscribed()) }
    fun emitsNext(vararg values: T): Verify<T>           = apply { steps.add(Step.EmitsNext(values.toList())) }
    fun emitsCount(count: Long): Verify<T>               = apply { steps.add(Step.EmitsCount(count)) }
    fun assertNext(assertion: (T) -> Unit): Verify<T>    = apply { steps.add(Step.MatchesNext(assertion)) }
    fun matchesNext(assertion: (T) -> Unit): Verify<T>   = assertNext(assertion)
    fun runs(action: () -> Unit): Verify<T>              = apply { steps.add(Step.Runs(action)) }
    /** Steps added after [thenCancels] are silently ignored — [thenCancels] breaks the step loop immediately. */
    fun thenCancels(): Verify<T>                         = apply { steps.add(Step.ThenCancels()) }

    /**
     * Executes the step list within [within] but does NOT assert that the stream completes.
     * Use [completesNormally] to additionally assert completion.
     */
    fun verify(within: Duration = timeout)            = runBlocking(context + CoroutineName("verify")) { execute(expectComplete = false, within) }
    fun completesNormally(within: Duration = timeout) = runBlocking(context + CoroutineName("verify")) { execute(expectComplete = true,  within) }
    fun completesWithError(within: Duration = timeout): Exception = runBlocking(context + CoroutineName("verify")) { completesWithErrorInternal(within) }

    /** Asserts the stream completes immediately with no items emitted. Fails if any item is emitted. */
    fun completesEmpty(within: Duration = timeout) = runBlocking(context + CoroutineName("verify")) {
        val items   = Channel<T>(Channel.UNLIMITED)
        var failed: T? = null
        val producer = producerScope().launch {
            drive(
                onNext     = { value -> failed = value; items.close(); Signal.Downstream.Cancel },
                onComplete = { items.close() },
                onError    = { items.close() },
            )
        }
        withTimeout(within) { for (ignored in items) { } }
        producer.cancelAndJoin()
        check(failed == null) { "expected empty stream but got item: $failed" }
    }

    private suspend fun completesWithErrorInternal(within: Duration): Exception {
        val items          = Channel<T>(Channel.UNLIMITED)
        var terminalCause: Exception = IllegalStateException("no error received")
        var hasError       = false
        val producer = producerScope().launch {
            drive(
                onNext     = { value -> items.trySend(value); Signal.Downstream.Request },
                onComplete = { items.close() },
                onError    = { cause -> terminalCause = cause; hasError = true; items.close() },
            )
        }
        withTimeout(within) { for (ignored in items) { } }
        producer.cancelAndJoin()
        if (!hasError) error("expected error but stream completed normally")
        return terminalCause
    }

    private suspend fun execute(expectComplete: Boolean, within: Duration) {
        val items       = Channel<T>(Channel.UNLIMITED)
        var terminal: Terminal = Complete
        var terminalSet = false

        val producer = producerScope().launch {
            drive(
                onNext     = { value ->
                    items.trySend(value)
                    if (terminalSet) Signal.Downstream.Cancel else Signal.Downstream.Request
                },
                onComplete = { if (!terminalSet) { terminal = Complete; terminalSet = true }; items.close() },
                onError    = { cause ->
                    terminal    = Errored(cause)
                    terminalSet = true
                    items.close()
                },
            )
        }

        for (step in steps) {
            when (step) {
                is Step.IsSubscribed -> Unit
                is Step.Runs         -> step.action()
                is Step.ThenCancels  -> { producer.cancelAndJoin(); terminal = Cancel; break }
                is Step.EmitsNext    -> for (expected in step.values) {
                    val actual = withTimeout(within) { items.receive() }
                    check(actual == expected) { "expected $expected but got $actual" }
                }
                is Step.EmitsCount   -> repeat(step.count.toInt()) { withTimeout(within) { items.receive() } }
                is Step.MatchesNext  -> {
                    val actual = withTimeout(within) { items.receive() }
                    step.assertion(actual)
                }
            }
        }

        if (expectComplete) {
            withTimeout(within) { while (!terminalSet) delay(1) }
            producer.cancelAndJoin()
            when (val t = terminal) {
                is Errored -> throw AssertionError("expected complete but got error: ${t.cause}", t.cause)
                Complete   -> Unit
                Cancel     -> Unit
            }
        } else {
            producer.cancelAndJoin()
        }
    }

    /**
     * Drives the publisher via [Many.source] when available, preserving the full coroutine
     * context. Falls back to the RS [Publisher] bridge only for genuinely external publishers.
     */
    private suspend fun drive(
        onNext:     suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError:    suspend (Exception) -> Unit,
    ) = try {
        when (val p = publisher) {
            is Many<*>  -> {
                @Suppress("UNCHECKED_CAST")
                (p as Many<T>).source(onNext, onComplete, onError)
            }
            is Maybe<*> -> {
                @Suppress("UNCHECKED_CAST")
                (p as Maybe<T>).source(onNext, onComplete, onError)
            }
            is One<*>   -> {
                @Suppress("UNCHECKED_CAST")
                (p as One<T>).source(onNext, onComplete, onError)
            }
            else -> {
                p.asFlow().collect { value -> onNext(value) }
                onComplete()
            }
        }
    } catch (e: Exception) {
        onError(e)
    }

    /**
     * Inherits the full coroutine context (including any [se.oyabun.minamoto.PoolContext] and
     * transaction state) but replaces the [Job] with an independent [SupervisorJob] so the
     * producer can be cancelled without propagating to the caller, and so [runBlocking] does
     * not wait for the producer to finish before returning.
     */
    private suspend fun producerScope(): CoroutineScope =
        CoroutineScope(currentCoroutineContext().minusKey(Job) + SupervisorJob())

    companion object {
        fun <T : Any> that(
            publisher: Publisher<T>,
            timeout:   Duration        = 5.seconds,
            context:   CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(publisher, timeout, context)

        fun <T : Any> that(
            one:     One<T>,
            timeout: Duration          = 5.seconds,
            context: CoroutineContext   = EmptyCoroutineContext,
        ): Verify<T> = Verify(one, timeout, context)

        fun <T : Any> that(
            maybe:   Maybe<T>,
            timeout: Duration          = 5.seconds,
            context: CoroutineContext   = EmptyCoroutineContext,
        ): Verify<T> = Verify(maybe, timeout, context)
    }
}

/**
 * Asserts that this [Maybe] is present and satisfies [assertion].
 * Fails if the [Maybe] is empty.
 */
fun <T : Any> Verify<T>.isPresent(assertion: (T) -> Unit = {}): Verify<T> =
    assertNext(assertion)

/**
 * Asserts that this [Maybe] completes without emitting a value.
 * Fails if a value is emitted.
 */
fun <T : Any> Verify<T>.isAbsent() = completesEmpty()
