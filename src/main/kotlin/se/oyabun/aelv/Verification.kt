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
    private data class Errored(val cause: Throwable) : Terminal

    private val steps = mutableListOf<Step<T>>()

    fun isSubscribed(): Verify<T>                       = apply { steps.add(Step.IsSubscribed()) }
    fun emitsNext(vararg values: T): Verify<T>          = apply { steps.add(Step.EmitsNext(values.toList())) }
    fun emitsCount(count: Long): Verify<T>              = apply { steps.add(Step.EmitsCount(count)) }
    fun matchesNext(assertion: (T) -> Unit): Verify<T>  = apply { steps.add(Step.MatchesNext(assertion)) }
    fun runs(action: () -> Unit): Verify<T>             = apply { steps.add(Step.Runs(action)) }
    fun thenCancels(): Verify<T>                        = apply { steps.add(Step.ThenCancels()) }

    fun verify()            = execute(expectComplete = false)
    fun completesNormally() = execute(expectComplete = true)

    fun completesWithError(): Throwable = runBlocking {
        val items = Channel<T>(Channel.UNLIMITED)
        var terminalCause: Throwable? = null

        publisher.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) { s.request(Long.MAX_VALUE) }
            override fun onNext(t: T)                 { items.trySend(t) }
            override fun onError(t: Throwable)        { terminalCause = t; runCatching { items.close() } }
            override fun onComplete()                 { runCatching { items.close() } }
        })

        withTimeout(timeout) { for (ignored in items) { } }
        terminalCause ?: error("expected error but stream completed normally")
    }

    private fun execute(expectComplete: Boolean) = runBlocking {
        val items      = Channel<T>(Channel.UNLIMITED)
        var terminal: Terminal? = null
        var subscription: Subscription? = null
        val subscribed = Channel<Unit>(1)

        publisher.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) {
                subscription = s
                subscribed.trySend(Unit)
                s.request(Long.MAX_VALUE)
            }
            override fun onNext(t: T)          { items.trySend(t) }
            override fun onError(t: Throwable) { terminal = Errored(t); runCatching { items.close() } }
            override fun onComplete()          { if (terminal == null) { terminal = Complete }; runCatching { items.close() } }
        })

        withTimeout(timeout) { subscribed.receive() }

        for (step in steps) {
            when (step) {
                is Step.IsSubscribed  -> Unit
                is Step.Runs          -> step.action()
                is Step.ThenCancels   -> { subscription?.cancel(); terminal = Cancel; break }
                is Step.EmitsNext     -> for (expected in step.values) {
                    val actual = withTimeout(timeout) { items.receive() }
                    check(actual == expected) { "expected $expected but got $actual" }
                }
                is Step.EmitsCount    -> repeat(step.count.toInt()) {
                    withTimeout(timeout) { items.receive() }
                }
                is Step.MatchesNext  -> {
                    val actual = withTimeout(timeout) { items.receive() }
                    step.assertion(actual)
                }
            }
        }

        if (expectComplete) {
            withTimeout(timeout) { while (terminal == null) delay(1) }
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
    }
}
