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
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
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
    private val timeout: Duration = 5.seconds,
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
    private data object Cancel : Terminal
    private data class Errored(val cause: Exception) : Terminal

    private val steps = mutableListOf<Step<T>>()

    fun isSubscribed(): Verify<T>                        = apply { steps.add(Step.IsSubscribed()) }
    fun emitsNext(vararg values: T): Verify<T>           = apply { steps.add(Step.EmitsNext(values.toList())) }
    fun emitsCount(count: Long): Verify<T>               = apply { steps.add(Step.EmitsCount(count)) }
    fun assertNext(assertion: (T) -> Unit): Verify<T>    = apply { steps.add(Step.MatchesNext(assertion)) }
    fun matchesNext(assertion: (T) -> Unit): Verify<T>   = assertNext(assertion)
    fun runs(action: () -> Unit): Verify<T>              = apply { steps.add(Step.Runs(action)) }
    fun thenCancels(): Verify<T>                         = apply { steps.add(Step.ThenCancels()) }

    fun verify(within: Duration = timeout)            = runBlocking { execute(expectComplete = false, within) }
    fun completesNormally(within: Duration = timeout) = runBlocking { execute(expectComplete = true,  within) }
    fun completesWithError(within: Duration = timeout): Throwable = runBlocking { completesWithErrorInternal(within) }

    /** Asserts the stream completes immediately with no items emitted. Fails if any item is emitted. */
    fun completesEmpty(within: Duration = timeout) = runBlocking {
        val completed  = Channel<Unit>(1)
        var failed: T? = null
        publisher.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE) }
            override fun onNext(t: T)                 { failed = t; runCatching { completed.close() } }
            override fun onError(t: Throwable)        { runCatching { completed.close() } }
            override fun onComplete()                 { completed.trySend(Unit); runCatching { completed.close() } }
        })
        withTimeout(within) { completed.receiveCatching() }
        check(failed == null) { "expected empty stream but got item: $failed" }
    }

    private suspend fun completesWithErrorInternal(within: Duration): Throwable {
        val items = Channel<T>(Channel.UNLIMITED)
        var terminalCause: Throwable = IllegalStateException("no error received")
        var hasError = false

        publisher.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE) }
            override fun onNext(t: T)                 { items.trySend(t) }
            override fun onError(t: Throwable)        { terminalCause = t; hasError = true; runCatching { items.close() } }
            override fun onComplete()                 { runCatching { items.close() } }
        })

        withTimeout(within) { for (ignored in items) { } }
        if (!hasError) error("expected error but stream completed normally")
        return terminalCause
    }

    private suspend fun execute(expectComplete: Boolean, within: Duration) {
        val items      = Channel<T>(Channel.UNLIMITED)
        var terminal: Terminal = Complete
        var terminalSet = false
        var subscription: SubscriptionState = SubscriptionState.Unbound
        val subscribed = Channel<Unit>(1)

        publisher.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) {
                subscription = SubscriptionState.Bound(s)
                subscribed.trySend(Unit)
                s.request(Long.MAX_VALUE)
            }
            override fun onNext(t: T)          { items.trySend(t) }
            override fun onError(t: Throwable) { terminal = Errored(if (t is Exception) t else RuntimeException(t)); terminalSet = true; runCatching { items.close() } }
            override fun onComplete()          { if (!terminalSet) { terminal = Complete; terminalSet = true }; runCatching { items.close() } }
        })

        withTimeout(within) { subscribed.receive() }

        for (step in steps) {
            when (step) {
                is Step.IsSubscribed  -> Unit
                is Step.Runs          -> step.action()
                is Step.ThenCancels   -> {
                    when (val state = subscription) {
                        is SubscriptionState.Bound   -> state.subscription.cancel()
                        is SubscriptionState.Unbound -> Unit
                    }
                    terminal = Cancel; break
                }
                is Step.EmitsNext     -> for (expected in step.values) {
                    val actual = withTimeout(within) { items.receive() }
                    check(actual == expected) { "expected $expected but got $actual" }
                }
                is Step.EmitsCount    -> repeat(step.count.toInt()) {
                    withTimeout(within) { items.receive() }
                }
                is Step.MatchesNext  -> {
                    val actual = withTimeout(within) { items.receive() }
                    step.assertion(actual)
                }
            }
        }

        if (expectComplete) {
            withTimeout(within) { while (!terminalSet && terminal == Complete) delay(1) }
            when (terminal) {
                is Errored -> throw AssertionError("expected complete but got error: ${(terminal as Errored).cause}", (terminal as Errored).cause)
                Complete   -> Unit
                else       -> check(false) { "expected complete but got $terminal" }
            }
        }
    }

    companion object {
        fun <T : Any> that(publisher: Publisher<T>, timeout: Duration = 5.seconds): Verify<T> =
            Verify(publisher, timeout)

        fun <T : Any> that(maybe: Maybe<T>, timeout: Duration = 5.seconds): Verify<T> =
            Verify(maybe, timeout)
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
