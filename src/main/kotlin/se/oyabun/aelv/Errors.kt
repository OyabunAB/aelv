package se.oyabun.aelv

sealed class Either<out A, out B> {
    data class Left<out A>(val value: A) : Either<A, Nothing>()
    data class Right<out B>(val value: B) : Either<Nothing, B>()

    fun isLeft(): Boolean = this is Left
    fun isRight(): Boolean = this is Right

    fun leftOrNull(): A? = if (this is Left) value else null
    fun rightOrNull(): B? = if (this is Right) value else null

    inline fun <C> fold(onLeft: (A) -> C, onRight: (B) -> C): C = when (this) {
        is Left -> onLeft(value)
        is Right -> onRight(value)
    }

    inline fun <C> mapLeft(transform: (A) -> C): Either<C, B> = when (this) {
        is Left -> Left(transform(value))
        is Right -> this
    }

    inline fun <C> mapRight(transform: (B) -> C): Either<A, C> = when (this) {
        is Left -> this
        is Right -> Right(transform(value))
    }
}

fun <A> A.left(): Either<A, Nothing> = Either.Left(this)
fun <B> B.right(): Either<Nothing, B> = Either.Right(this)

sealed class AelvException(message: String, cause: Throwable? = null) : IllegalArgumentException(message, cause)

class InvalidDemandException(n: Long) :
    AelvException("request must be positive, got $n (RS spec §3.9)")

class NoSuchElementException :
    AelvException("stream completed without emitting a value")

class UpstreamErrorException(cause: Throwable) :
    AelvException("upstream publisher signalled an error", cause)

class OperatorException(operator: String, cause: Throwable) :
    AelvException("[$operator] ${cause.message ?: cause::class.simpleName}", cause)
