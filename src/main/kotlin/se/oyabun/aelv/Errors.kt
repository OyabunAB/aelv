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
 * A value of either type [A] (left) or type [B] (right).
 *
 * Used throughout aelv as the return type of terminal operators: the left side carries the
 * success value; the right side carries an [AelvException].
 */
sealed class Either<out A, out B> {
    /** The successful outcome, carrying [value] of type [A]. */
    data class Left<out A>(val value: A) : Either<A, Nothing>()
    /** The failure or secondary outcome, carrying [value] of type [B]. */
    data class Right<out B>(val value: B) : Either<Nothing, B>()

    /** Returns true if this is a [Left]. */
    fun isLeft(): Boolean = this is Left
    /** Returns true if this is a [Right]. */
    fun isRight(): Boolean = this is Right

    /** Returns the [Left] value, or null if this is a [Right]. */
    fun leftOrNull(): A? = if (this is Left) value else null
    /** Returns the [Right] value, or null if this is a [Left]. */
    fun rightOrNull(): B? = if (this is Right) value else null

    /** Returns the [Left] value, or throws the [Right] value if it is a [Throwable], otherwise throws [IllegalStateException]. */
    fun leftOrThrow(): A = when (this) {
        is Left  -> value
        is Right -> throw if (value is Throwable) value else IllegalStateException("Either.Right: $value")
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

    /** Transforms the [Right] value with [transform], leaving [Left] unchanged. */
    inline fun <C> mapRight(transform: (B) -> C): Either<A, C> = when (this) {
        is Left -> this
        is Right -> Right(transform(value))
    }
}

/** Wraps this value as [Either.Left]. */
fun <A> A.left(): Either<A, Nothing> = Either.Left(this)
/** Wraps this value as [Either.Right]. */
fun <B> B.right(): Either<Nothing, B> = Either.Right(this)

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

/** Thrown by terminal operators such as [Many.first] and [One.get] when the stream is empty. */
class NoSuchElementException :
    AelvException("stream completed without emitting a value")

/** Wraps a non-[AelvException] throwable received from an upstream publisher. */
class UpstreamErrorException(cause: Throwable) :
    AelvException("upstream publisher signalled an error", cause)
