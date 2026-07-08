package se.oyabun.aelv

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore

fun <T : Any, R : Any> Many<T>.map(transform: (T) -> R): Many<R> =
    Many.generate { emit -> this.source { emit(transform(it)) } }

fun <T : Any> Many<T>.filter(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit -> this.source { if (predicate(it)) emit(it) } }

fun <T : Any> Many<T>.take(n: Long): Many<T> {
    require(n >= 0) { "take count must be non-negative, got $n" }
    return Many.generate { emit ->
        var remaining = n
        this.source { item -> if (remaining > 0L) { emit(item); remaining-- } }
    }
}

fun <T : Any> Many<T>.takeWhile(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit -> this.source { if (predicate(it)) emit(it) } }

fun <T : Any> Many<T>.skip(n: Long): Many<T> {
    require(n >= 0) { "skip count must be non-negative, got $n" }
    return Many.generate { emit ->
        var skipped = 0L
        this.source { item -> if (skipped < n) skipped++ else emit(item) }
    }
}

fun <T : Any> Many<T>.skipWhile(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit ->
        var skipping = true
        this.source { item ->
            if (skipping && predicate(item)) return@source
            skipping = false
            emit(item)
        }
    }

fun <T : Any> Many<T>.distinct(): Many<T> =
    Many.generate { emit ->
        val seen = HashSet<T>()
        this.source { if (seen.add(it)) emit(it) }
    }

@Suppress("UNCHECKED_CAST")
fun <T : Any> Many<T>.distinctUntilChanged(): Many<T> =
    Many.generate { emit ->
        var last: Any = Unset
        this.source { item -> if (item != last) { last = item; emit(item) } }
    }

@Suppress("UNCHECKED_CAST")
fun <T : Any, K : Any> Many<T>.distinctUntilChangedBy(key: (T) -> K): Many<T> =
    Many.generate { emit ->
        var lastKey: Any = Unset
        this.source { item ->
            val k = key(item)
            if (k != lastKey) { lastKey = k; emit(item) }
        }
    }

fun <T : Any, R : Any> Many<T>.flatMap(
    concurrency: Int = Int.MAX_VALUE,
    transform: (T) -> Many<R>,
): Many<R> = Many.generate { emit ->
    val semaphore = if (concurrency < Int.MAX_VALUE) Semaphore(concurrency) else null
    coroutineScope {
        this@flatMap.source { item ->
            semaphore?.acquire()
            launch {
                try { transform(item).source { emit(it) } }
                finally { semaphore?.release() }
            }
        }
    }
}

fun <T : Any, R : Any> Many<T>.concatMap(transform: (T) -> Many<R>): Many<R> =
    flatMap(concurrency = 1, transform = transform)

fun <T : Any, R : Any> Many<T>.switchMap(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        coroutineScope {
            var activeJob: Job? = null
            this@switchMap.source { item ->
                activeJob?.cancelAndJoin()
                activeJob = launch { transform(item).source { emit(it) } }
            }
            activeJob?.join()
        }
    }

fun <T : Any> Many<T>.mergeWith(other: Many<T>): Many<T> =
    Many.generate { emit ->
        coroutineScope {
            launch { this@mergeWith.source { emit(it) } }
            launch { other.source { emit(it) } }
        }
    }

fun <T : Any> merge(vararg sources: Many<T>): Many<T> =
    Many.generate { emit ->
        coroutineScope { sources.forEach { source -> launch { source.source { emit(it) } } } }
    }

fun <T : Any> concat(vararg sources: Many<T>): Many<T> =
    Many.generate { emit -> for (source in sources) source.source { emit(it) } }

fun <A : Any, B : Any, R : Any> zip(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        val channelA = Channel<A>(Channel.BUFFERED)
        val channelB = Channel<B>(Channel.BUFFERED)
        coroutineScope {
            launch { a.source { channelA.send(it) }; channelA.close() }
            launch { b.source { channelB.send(it) }; channelB.close() }
            for (itemA in channelA) {
                val result = channelB.receiveCatching()
                if (result.isClosed) break
                emit(transform(itemA, result.getOrThrow()))
            }
        }
    }

@Suppress("UNCHECKED_CAST")
fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        var latestA: Any = Unset
        var latestB: Any = Unset
        coroutineScope {
            launch {
                a.source { value ->
                    latestA = value
                    if (latestB !== Unset) emit(transform(value, latestB as B))
                }
            }
            launch {
                b.source { value ->
                    latestB = value
                    if (latestA !== Unset) emit(transform(latestA as A, value))
                }
            }
        }
    }

fun <T : Any> Many<T>.buffer(size: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    return Many.generate { emit ->
        val bucket = mutableListOf<T>()
        this.source { item ->
            bucket.add(item)
            if (bucket.size == size) { emit(bucket.toList()); bucket.clear() }
        }
        if (bucket.isNotEmpty()) emit(bucket.toList())
    }
}

fun <T : Any> Many<T>.buffer(size: Int, skip: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    require(skip > 0) { "buffer skip must be positive, got $skip" }
    return Many.generate { emit ->
        val buffers = ArrayDeque<MutableList<T>>()
        var index = 0L
        this.source { item ->
            if (index % skip == 0L) buffers.addLast(mutableListOf())
            buffers.forEach { it.add(item) }
            val full = buffers.firstOrNull { it.size == size }
            if (full != null) { emit(full.toList()); buffers.removeFirst() }
            index++
        }
    }
}

data class GroupedMany<K : Any, T : Any>(val key: K, val many: Many<T>)

fun <T : Any, K : Any> Many<T>.groupBy(keySelector: (T) -> K): Many<GroupedMany<K, T>> =
    Many.generate { emit ->
        val groups = mutableMapOf<K, suspend (T) -> Unit>()
        val groupEmitters = mutableMapOf<K, MutableList<T>>()
        this.source { item ->
            val key = keySelector(item)
            if (!groupEmitters.containsKey(key)) {
                val bucket = mutableListOf<T>()
                groupEmitters[key] = bucket
                val group = Many.generate<T> { groupEmit ->
                    for (buffered in bucket) groupEmit(buffered)
                    groups[key] = groupEmit
                }
                emit(GroupedMany(key, group))
            }
            val emitter = groups[key]
            if (emitter != null) emitter(item) else groupEmitters[key]?.add(item)
        }
    }

enum class SignalType { COMPLETE, ERROR, CANCEL }

fun <T : Any> Many<T>.doOnNext(action: (T) -> Unit): Many<T> =
    Many.generate { emit -> this.source { action(it); emit(it) } }

fun <T : Any> Many<T>.doOnComplete(action: () -> Unit): Many<T> =
    Many.generate { emit -> this.source { emit(it) }; action() }

fun <T : Any> Many<T>.doOnError(action: (Throwable) -> Unit): Many<T> =
    Many.generate { emit ->
        try { this.source { emit(it) } }
        catch (e: Throwable) { action(e); throw e }
    }

fun <T : Any> Many<T>.doOnSubscribe(action: () -> Unit): Many<T> =
    Many.generate { emit -> action(); this.source { emit(it) } }

fun <T : Any> Many<T>.doFinally(action: (SignalType) -> Unit): Many<T> =
    Many.generate { emit ->
        try { this.source { emit(it) }; action(SignalType.COMPLETE) }
        catch (e: CancellationException) { action(SignalType.CANCEL); throw e }
        catch (e: Throwable) { action(SignalType.ERROR); throw e }
    }

fun <T : Any> Many<T>.recover(fallback: (AelvException) -> Many<T>): Many<T> =
    Many.generate { emit ->
        val result = this.collect { emit(it) }
        if (result is Either.Right) fallback(result.value).source { emit(it) }
    }

fun <T : Any> Many<T>.recoverWith(fallback: (AelvException) -> T): Many<T> =
    Many.generate { emit ->
        val result = this.collect { emit(it) }
        if (result is Either.Right) emit(fallback(result.value))
    }

fun <T : Any> Many<T>.retry(times: Long = Long.MAX_VALUE): Many<T> =
    Many.generate { emit ->
        var attempts = 0L
        while (true) {
            val result = this.collect { emit(it) }
            when {
                result is Either.Left -> break
                attempts >= times -> throw (result as Either.Right).value
                else -> attempts++
            }
        }
    }

fun <T : Any, R : Any> One<T>.map(transform: (T) -> R): One<R> =
    One.generate { emit -> this.source { emit(transform(it)) } }

fun <T : Any, R : Any> One<T>.flatMap(transform: (T) -> One<R>): One<R> =
    One.generate { emit -> this.source { transform(it).source { r -> emit(r) } } }

fun <T : Any, R : Any> One<T>.flatMapMany(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit -> this.source { transform(it).source { r -> emit(r) } } }

fun <T : Any> One<T>.flatMapNone(transform: (T) -> None<T>): None<T> =
    None.generate { this.source { transform(it).await() } }

fun <T : Any> One<T>.recover(fallback: (AelvException) -> T): One<T> =
    One.generate { emit ->
        val result = this.collect { emit(it) }
        if (result is Either.Right) emit(fallback(result.value))
    }

fun <T : Any> One<T>.retry(times: Long = Long.MAX_VALUE): One<T> =
    One.generate { emit ->
        var attempts = 0L
        while (true) {
            val result = this.collect { emit(it) }
            when {
                result is Either.Left -> break
                attempts >= times -> throw (result as Either.Right).value
                else -> attempts++
            }
        }
    }

fun <T : Any> One<T>.doOnNext(action: (T) -> Unit): One<T> =
    One.generate { emit -> this.source { action(it); emit(it) } }

fun <T : Any> One<T>.doOnError(action: (AelvException) -> Unit): One<T> =
    One.generate { emit ->
        val result = this.collect { emit(it) }
        if (result is Either.Right) { action(result.value); throw result.value }
    }

fun <T : Any> One<T>.doFinally(action: (SignalType) -> Unit): One<T> =
    One.generate { emit ->
        val result = this.collect { emit(it) }
        action(if (result is Either.Left) SignalType.COMPLETE else SignalType.ERROR)
        if (result is Either.Right) throw result.value
    }

suspend fun <T : Any> One<T>.get(): Either<T, AelvException> {
    var result: Any = Unset
    val outcome = this.collect { result = it }
    return when {
        result !== Unset -> {
            @Suppress("UNCHECKED_CAST")
            (result as T).left()
        }
        outcome is Either.Right -> outcome
        else -> NoSuchElementException().right()
    }
}
