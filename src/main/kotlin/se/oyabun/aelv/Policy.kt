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

import kotlin.random.Random
import kotlin.reflect.KClass
import kotlin.time.Duration

/**
 * Backoff strategy between retry attempts.
 */
sealed class Backoff {
    /** No delay between retry attempts. */
    data object None : Backoff()

    /** A constant [delay] between every retry attempt. */
    data class Fixed(val delay: Duration) : Backoff()

    /**
     * Exponential backoff: delay doubles each attempt up to [max].
     * When [jitter] is true, full jitter is applied — actual delay is
     * uniform random in [0, computed], avoiding thundering herd.
     */
    data class Exponential(
        val initial: Duration,
        val max: Duration,
        val factor: Double = 2.0,
        val jitter: Boolean = false,
    ) : Backoff()
}

internal fun Backoff.delayFor(attempt: Long): Duration = when (this) {
    is Backoff.None -> Duration.ZERO
    is Backoff.Fixed -> delay
    is Backoff.Exponential -> {
        val computed = minOf(max, initial * Math.pow(factor, attempt.toDouble()))
        if (jitter) computed * Random.nextDouble() else computed
    }
}

/**
 * Resilience policies. Use [Policy.retry] to build a retry policy.
 *
 * ```kotlin
 * stream.retry(
 *     Policy.retry()
 *         .on(IOException::class)
 *         .withBackoff(1.seconds, 30.seconds, jitter = true)
 *         .maxAttempts(5)
 * )
 * ```
 */
sealed class Policy {

    /**
     * Retry policy.
     *
     * Build via [Policy.retry]:
     * ```kotlin
     * Policy.retry()
     *     .on(IOException::class)
     *     .withBackoff(1.seconds, 30.seconds, jitter = true)
     *     .maxAttempts(5)
     * ```
     */
    @ConsistentCopyVisibility
    data class Retry internal constructor(
        internal val maxAttempts: Long = Long.MAX_VALUE,
        internal val backoff: Backoff = Backoff.None,
        internal val filter: (Throwable) -> Boolean = { true },
    ) : Policy() {

        /** Only retry when the thrown exception is an instance of [type]. */
        fun on(type: KClass<out Throwable>): Retry = copy(filter = { type.isInstance(it) })

        /** Only retry when [predicate] returns true for the thrown exception. */
        fun on(predicate: (Throwable) -> Boolean): Retry = copy(filter = predicate)

        /** Fixed delay between every attempt. */
        fun withBackoff(delay: Duration): Retry {
            require(delay.isPositive()) { "backoff delay must be positive" }
            return copy(backoff = Backoff.Fixed(delay))
        }

        /**
         * Exponential backoff from [initial] up to [max].
         * When [jitter] is true, full jitter is applied — actual delay is
         * uniform random in [0, computed], avoiding thundering herd.
         */
        fun withBackoff(
            initial: Duration,
            max: Duration,
            factor: Double = 2.0,
            jitter: Boolean = false,
        ): Retry {
            require(initial.isPositive()) { "backoff initial must be positive" }
            require(max >= initial) { "backoff max must be >= initial" }
            require(factor > 1.0) { "backoff factor must be > 1.0, got $factor" }
            return copy(backoff = Backoff.Exponential(initial, max, factor, jitter))
        }

        /** Maximum number of retry attempts after the first failure. 0 means no retries. */
        fun maxAttempts(n: Long): Retry {
            require(n >= 0) { "maxAttempts must be non-negative, got $n" }
            return copy(maxAttempts = n)
        }
    }

    companion object {
        /** Start building a [Retry] policy. */
        fun retry(): Retry = Retry()
    }
}
