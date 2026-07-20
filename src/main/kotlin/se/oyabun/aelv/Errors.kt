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

/**
 * A value of either type [A] (left — error) or type [B] (right — success).
 *
 * Convention: [Left] carries the error; [Right] carries the success value.
 * This aligns with the standard FP convention (Arrow, Haskell, Scala) where
 * "right" means correct.
 *
 * Used throughout aelv as the return type of terminal operators.
 */
sealed class Either<out A, out B> {
    /** The error outcome, carrying [value] of type [A]. */
    data class Left<out A>(val value: A) : Either<A, Nothing>()
    /** The success outcome, carrying [value] of type [B]. */
    data class Right<out B>(val value: B) : Either<Nothing, B>()

    /** Returns true if this is a [Left]. */
    fun isLeft(): Boolean = this is Left
    /** Returns true if this is a [Right]. */
    fun isRight(): Boolean = this is Right
    /** Returns true if this is a [Right] — semantic alias for [isRight]. */
    fun isOk(): Boolean = this is Right
    /** Returns true if this is a [Left] — semantic alias for [isLeft]. */
    fun isError(): Boolean = this is Left

    /** Returns the [Left] value, or null if this is a [Right]. */
    fun leftOrNull(): A? = if (this is Left) value else null
    /** Returns the [Right] value, or null if this is a [Left]. */
    fun rightOrNull(): B? = if (this is Right) value else null
    /** Returns the success value, or null if this is an error — semantic alias for [rightOrNull]. */
    fun okOrNull(): B? = if (this is Right) value else null
    /** Returns the error value, or null if this is a success — semantic alias for [leftOrNull]. */
    fun errorOrNull(): A? = if (this is Left) value else null

    /** Returns the [Left] value. If this is a [Right], throws the right value when it is a [Throwable]; otherwise wraps it in [IllegalStateException]. */
    fun leftOrThrow(): A = when (this) {
        is Left  -> value
        is Right -> throw if (value is Throwable) value else IllegalStateException("Either.Left: $value")
    }

    /** Collapses both sides to [C] by applying [onLeft] or [onRight]. */
    inline fun <C> fold(onLeft: (A) -> C, onRight: (B) -> C): C = when (this) {
        is Left -> onLeft(value)
        is Right -> onRight(value)
    }

    /** Transforms the [Left] value with [transform], leaving [Right] unchanged. */
    inline fun <C> mapLeft(transform: (A) -> C): Either<C, B> = when (this) {
        is Left -> Left(transform(value))
        is Right -> this
    }

    /** Runs [block] with the [Left] value if this is a [Left], then returns [this] unchanged. */
    inline fun onLeft(block: (A) -> Unit): Either<A, B> = also { if (it is Left) block(it.value) }

    /** Transforms the [Right] value with [transform], leaving [Left] unchanged. */
    inline fun <C> mapRight(transform: (B) -> C): Either<A, C> = when (this) {
        is Left -> this
        is Right -> Right(transform(value))
    }

    companion object {
        suspend inline fun <T> catching(block: suspend () -> T): Either<Exception, T> =
            try { block().right() } catch (issue: Exception) { issue.leftUnlessCancelled() }

        inline fun <T> catchingStrict(block: () -> T): Either<Exception, T> =
            try { block().right() } catch (issue: Exception) { issue.left() }

        suspend fun <T> catching(timeout: kotlin.time.Duration, block: suspend () -> T): Either<Exception, T> =
            try { kotlinx.coroutines.withTimeout(timeout) { block() }.right() }
            catch (issue: kotlinx.coroutines.TimeoutCancellationException) { TimeoutException(timeout).left() }
            catch (issue: Exception) { if (issue is kotlinx.coroutines.CancellationException) throw issue; issue.left() }

        /** Constructs a success outcome — alias for [Either.Right]. */
        fun <T> success(value: T): Either<Nothing, T> = Right(value)
        /** Constructs a failure outcome — alias for [Either.Left]. */
        fun <E> failure(error: E): Either<E, Nothing> = Left(error)
    }
}

/** Wraps this value as [Either.Right] — the success side. */
fun <B> B.right(): Either<Nothing, B> = Either.Right(this)
/** Wraps this value as [Either.Left] — the error side. */
fun <A> A.left(): Either<A, Nothing> = Either.Left(this)

typealias Success<T> = Either.Right<T>
typealias Failure<E> = Either.Left<E>

/**
 * Returns the [Right] value, or throws the [Left] value if it is a [Throwable],
 * otherwise throws [IllegalStateException].
 */
fun <A : Throwable, B> Either<A, B>.rightOrThrow(): B = when (this) {
    is Success -> value
    is Failure  -> throw value
}

/** Alias for [rightOrThrow] — matches Kotlin's [Result.getOrThrow] convention. */
fun <A : Throwable, B> Either<A, B>.getOrThrow(): B = rightOrThrow()

/**
 * Base class for all exceptions thrown or signalled by aelv.
 *
 * Extends [IllegalArgumentException] to satisfy Reactive Streams §3.9, which requires that
 * `request(n <= 0)` results in an `IllegalArgumentException`.
 */
sealed class AelvException(message: String, cause: Throwable? = null) : IllegalArgumentException(message, cause)

/** Thrown when a subscriber calls `request(n)` with `n <= 0` (RS spec §3.9). */
class InvalidDemandException(n: Long) :
    AelvException("request must be positive, got $n (RS spec §3.9)")

/** Thrown by terminal operators such as [Many.first] and [One.await] when the stream is empty. */
class NoSuchElementException :
    AelvException("stream completed without emitting a value")

/** Signals that a [One.await] call did not receive a value within the specified duration. */
class TimeoutException(timeout: kotlin.time.Duration) :
    AelvException("await timed out after $timeout")

/** Base class for all assertion failures thrown by [Verify]. */
sealed class VerifyAssertionError(message: String, cause: Throwable? = null) : AssertionError(message, cause)

/** Thrown when the stream terminated with the wrong [Signal.Terminal]. */
class UnexpectedTerminalError(expected: String, got: Signal.Terminal) :
    VerifyAssertionError(
        when (got) {
            Signal.Upstream.Complete -> "expected $expected but stream completed normally"
            Signal.Downstream.Cancel -> "expected $expected but stream was cancelled"
            is Signal.Upstream.Error -> "expected $expected but got error: ${got.cause.message}"
        },
        if (got is Signal.Upstream.Error) got.cause else null,
    )

/** Thrown when the error type does not match the expected type in [Verify.failedWith]. */
class UnexpectedErrorTypeError(expected: Class<*>, got: Exception) :
    VerifyAssertionError("expected ${expected.simpleName} but got ${got::class.simpleName}: ${got.message}")

/** Thrown when [Verify.assertNext] or [Verify.emitsNext] receives an unexpected value. */
class UnexpectedValueError(message: String) : VerifyAssertionError(message)

