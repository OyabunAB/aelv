package se.oyabun.aelv

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow as publisherAsFlow
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

class Many<T : Any> private constructor(
    internal val source: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val subscription = StreamSubscription(subscriber, scope, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            scope.cancel()
            throw e
        }
    }

    fun asFlow(): Flow<T> = flow {
        var error: AelvException? = null
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { emit(signal.value); Signal.Downstream.Request(1) }
                is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
            }
        }
        error?.let { throw it }
    }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        var error: AelvException? = null
        try {
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> action(signal.value)
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
                }
            }
        } catch (e: AelvException) {
            error = e
        }
        return error?.right() ?: Unit.left()
    }

    companion object {

        private val log = Logging.of<Many<*>>()

        fun <T : Any> of(vararg items: T): Many<T> = Many { emit ->
            for (item in items) {
                if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@Many
            }
            emit(Signal.Upstream.Complete)
        }

        fun <T : Any> of(iterable: Iterable<T>): Many<T> = Many { emit ->
            for (item in iterable) {
                if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@Many
            }
            emit(Signal.Upstream.Complete)
        }

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): Many<T> = Many(block)

        fun <T : Any> from(flow: Flow<T>): Many<T> = Many { emit ->
            try {
                coroutineScope {
                    flow.collect { item ->
                        if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) cancel()
                    }
                }
                emit(Signal.Upstream.Complete)
            } catch (_: CancellationException) {}
        }

        fun <T : Any> from(publisher: Publisher<T>): Many<T> = from(publisher.publisherAsFlow())

        fun <T : Any> empty(): Many<T> = Many { emit -> emit(Signal.Upstream.Complete) }

        fun <T : Any> error(cause: AelvException): Many<T> = Many { emit ->
            emit(Signal.Upstream.Error(cause))
        }

        fun <T : Any> never(): Many<T> = Many { awaitCancellation() }
    }
}

class One<T : Any> private constructor(
    internal val source: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val subscription = StreamSubscription(subscriber, scope, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            scope.cancel()
            throw e
        }
    }

    fun asFlow(): Flow<T> = flow {
        var error: AelvException? = null
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { emit(signal.value); Signal.Downstream.Request(1) }
                is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
            }
        }
        error?.let { throw it }
    }

    fun asMany(): Many<T> = Many.generate { emit -> source { emit(it) } }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        var error: AelvException? = null
        try {
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> action(signal.value)
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
                }
            }
        } catch (e: AelvException) {
            error = e
        }
        return error?.right() ?: Unit.left()
    }

    companion object {

        private val log = Logging.of<One<*>>()

        fun <T : Any> of(value: T): One<T> = One { emit ->
            if (emit(Signal.Upstream.Next(value)) != Signal.Downstream.Cancel)
                emit(Signal.Upstream.Complete)
        }

        fun <T : Any> defer(block: suspend () -> T): One<T> = One { emit ->
            if (emit(Signal.Upstream.Next(block())) != Signal.Downstream.Cancel)
                emit(Signal.Upstream.Complete)
        }

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): One<T> = One(block)

        fun <T : Any> from(publisher: Publisher<T>): One<T> = One { emit ->
            try {
                coroutineScope {
                    publisher.publisherAsFlow().collect { value ->
                        emit(Signal.Upstream.Next(value))
                        cancel()
                    }
                }
            } catch (_: CancellationException) {}
            emit(Signal.Upstream.Complete)
        }

        fun <T : Any> error(cause: AelvException): One<T> = One { emit ->
            emit(Signal.Upstream.Error(cause))
        }

        fun <T : Any> never(): One<T> = One { awaitCancellation() }
    }
}

class None<T : Any> private constructor(
    private val source: suspend () -> Unit,
) : Publisher<Nothing> {

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val subscription = CompletionSubscription(subscriber, scope, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            scope.cancel()
            throw e
        }
    }

    internal suspend fun await(): Either<Unit, AelvException> = try {
        source()
        Unit.left()
    } catch (e: AelvException) {
        e.right()
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
