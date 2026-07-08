package se.oyabun.aelv

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow as publisherAsFlow
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

class Many<T : Any> private constructor(
    internal val source: suspend (emit: suspend (T) -> Unit) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        subscriber.onSubscribe(StreamSubscription(subscriber, scope, source))
    }

    fun asFlow(): Flow<T> = flow { source { emit(it) } }

    internal suspend fun collect(action: suspend (T) -> Unit): Either<Unit, AelvException> = try {
        source(action)
        Unit.left()
    } catch (e: AelvException) {
        e.right()
    } catch (e: Throwable) {
        UpstreamErrorException(e).right()
    }

    companion object {

        private val log = Logging.of<Many<*>>()

        fun <T : Any> of(vararg items: T): Many<T> = Many { emit ->
            for (item in items) emit(item)
        }

        fun <T : Any> of(iterable: Iterable<T>): Many<T> = Many { emit ->
            for (item in iterable) emit(item)
        }

        internal fun <T : Any> generate(block: suspend (emit: suspend (T) -> Unit) -> Unit): Many<T> = Many(block)

        fun <T : Any> from(flow: Flow<T>): Many<T> = Many { emit ->
            flow.collect { emit(it) }
        }

        fun <T : Any> from(publisher: Publisher<T>): Many<T> = from(publisher.publisherAsFlow())

        fun <T : Any> empty(): Many<T> = Many { }

        fun <T : Any> error(cause: AelvException): Many<T> = Many { throw cause }

        fun <T : Any> never(): Many<T> = Many { awaitCancellation() }
    }
}

class One<T : Any> private constructor(
    internal val source: suspend (emit: suspend (T) -> Unit) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        subscriber.onSubscribe(StreamSubscription(subscriber, scope, source))
    }

    fun asFlow(): Flow<T> = flow { source { emit(it) } }

    fun asMany(): Many<T> = Many.generate { emit -> source { emit(it) } }

    internal suspend fun collect(action: suspend (T) -> Unit): Either<Unit, AelvException> = try {
        source(action)
        Unit.left()
    } catch (e: AelvException) {
        e.right()
    } catch (e: Throwable) {
        UpstreamErrorException(e).right()
    }

    companion object {

        private val log = Logging.of<One<*>>()

        fun <T : Any> of(value: T): One<T> = One { emit -> emit(value) }

        fun <T : Any> defer(block: suspend () -> T): One<T> = One { emit -> emit(block()) }

        internal fun <T : Any> generate(block: suspend (emit: suspend (T) -> Unit) -> Unit): One<T> = One(block)

        fun <T : Any> from(publisher: Publisher<T>): One<T> = One { emit ->
            publisher.publisherAsFlow().collect { value ->
                emit(value)
                return@collect
            }
        }

        fun <T : Any> error(cause: AelvException): One<T> = One { throw cause }

        fun <T : Any> never(): One<T> = One { awaitCancellation() }
    }
}

class None<T : Any> private constructor(
    private val source: suspend () -> Unit,
) : Publisher<Nothing> {

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        subscriber.onSubscribe(CompletionSubscription(subscriber, scope, source))
    }

    internal suspend fun await(): Either<Unit, AelvException> = try {
        source()
        Unit.left()
    } catch (e: AelvException) {
        e.right()
    } catch (e: Throwable) {
        UpstreamErrorException(e).right()
    }

    companion object {

        private val log = Logging.of<None<*>>()

        fun <T : Any> defer(block: suspend () -> Unit): None<T> = None(block)

        internal fun <T : Any> generate(block: suspend () -> Unit): None<T> = None(block)

        fun <T : Any> from(publisher: Publisher<T>): None<T> = None {
            publisher.publisherAsFlow().collect { }
        }

        fun <T : Any> complete(): None<T> = None { }

        fun <T : Any> error(cause: AelvException): None<T> = None { throw cause }

        fun <T : Any> never(): None<T> = None { awaitCancellation() }
    }
}
