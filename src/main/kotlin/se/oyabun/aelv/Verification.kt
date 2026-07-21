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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class Verify<T : Any> private constructor(
    private val pipeline:   Observable<T, *>,
    private val context:    CoroutineContext = EmptyCoroutineContext,
    private val assertions: List<(List<T>) -> Unit> = emptyList(),
) {

    fun assertNext(assertion: (T) -> Unit): Verify<T> =
        Verify(pipeline, context, assertions + { items ->
            if (items.isEmpty()) throw UnexpectedValueError("expected at least one item but stream was empty")
            assertion(items.first())
        })

    fun emitsNext(vararg values: T): Verify<T> =
        Verify(pipeline, context, assertions + { items ->
            val actual = items.take(values.size)
            if (actual != values.toList()) throw UnexpectedValueError("expected ${values.toList()} but got $actual")
        })

    fun emitsCount(count: Long): Verify<T> =
        Verify(pipeline, context, assertions + { items ->
            if (items.size.toLong() != count) throw UnexpectedValueError("expected $count items but got ${items.size}")
        })

    private fun runPipeline(within: Duration): Signal.Terminal {
        val items = mutableListOf<T>()
        val terminal = runBlocking(context + CoroutineName("verify")) {
            withTimeout(within) {
                var terminal: Signal.Terminal = Signal.Downstream.Cancel
                pipeline.source(
                    { value -> items.add(value); Signal.Downstream.Request },
                    { terminal = Signal.Upstream.Complete },
                    { issue -> terminal = Signal.Upstream.Error(issue) },
                )
                terminal
            }
        }
        assertions.forEach { it(items) }
        return terminal
    }

    fun completes(within: Duration = DEFAULT_TIMEOUT) =
        when (val s = runPipeline(within)) {
            Signal.Upstream.Complete -> Unit
            else                     -> throw UnexpectedTerminalError("Complete", s)
        }

    fun cancels(within: Duration = DEFAULT_TIMEOUT) =
        when (val s = runPipeline(within)) {
            Signal.Downstream.Cancel -> Unit
            else                     -> throw UnexpectedTerminalError("Cancel", s)
        }

    fun fails(within: Duration = DEFAULT_TIMEOUT) =
        when (val s = runPipeline(within)) {
            is Signal.Upstream.Error -> Unit
            else                     -> throw UnexpectedTerminalError("Error", s)
        }

    inline fun <reified X : Exception> failsWith(within: Duration = DEFAULT_TIMEOUT, noinline assertions: (X) -> Unit = {}) =
        failsWith(X::class.java, within, assertions)

    @Suppress("UNCHECKED_CAST")
    fun <X : Exception> failsWith(type: Class<X>, within: Duration = DEFAULT_TIMEOUT, assertions: (X) -> Unit = {}) =
        when (val s = runPipeline(within)) {
            is Signal.Upstream.Error -> {
                if (!type.isInstance(s.cause)) throw UnexpectedErrorTypeError(type, s.cause)
                assertions(type.cast(s.cause))
            }
            else -> throw UnexpectedTerminalError(type.simpleName, s)
        }

    fun timesOut(within: Duration = DEFAULT_TIMEOUT) = failsWith<ExceededTimeoutException>(within)

    companion object {
        val DEFAULT_TIMEOUT get() = Aelv.verifyTimeout

        fun <T : Any> that(
            source:  Observable<T, *>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(source, context)
    }
}
