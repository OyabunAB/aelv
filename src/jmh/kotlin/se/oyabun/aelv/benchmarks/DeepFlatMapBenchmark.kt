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
package se.oyabun.aelv.benchmarks

import io.reactivex.rxjava3.core.Observable as RxObservable
import io.smallrye.mutiny.Multi
import kotlinx.coroutines.runBlocking
import monix.execution.Scheduler
import monix.reactive.Observable as MonixObservable
import org.openjdk.jmh.annotations.*
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.publisher.Flux
import se.oyabun.aelv.Many
import se.oyabun.aelv.await
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.take
import se.oyabun.aelv.toList
import java.util.concurrent.CountDownLatch

/**
 * Recursive concatMap stack-safety benchmark.
 *
 * step(0) = items(42)
 * step(n) = items(42).concatMap { step(n-1) }
 *
 * Assembly is O(1) — the lambda is lazy. Execution recurses O(depth) deep at
 * subscription time. Returns [OVERFLOW] (-1) when a library cannot handle the depth.
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class DeepFlatMapBenchmark {

    @Param("1000", "10000", "100000")
    var depth: Int = 1000

    @Benchmark
    fun aelv_deep_flatMap(): Int =
        runBlocking {
            fun step(n: Int): Many<Int> =
                if (n <= 0) Many.items(42)
                else Many.items(42).concatMap { step(n - 1) }
            step(depth).take(1).toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun rxjava_deep_flatMap(): Int = try {
        fun step(n: Int): RxObservable<Int> =
            if (n <= 0) RxObservable.just(42)
            else RxObservable.just(42).flatMap { step(n - 1) }
        step(depth).take(1).blockingFirst()!!
    } catch (_: Throwable) { OVERFLOW }

    @Benchmark
    fun reactor_deep_flatMap(): Int = try {
        fun step(n: Int): Flux<Int> =
            if (n <= 0) Flux.just(42)
            else Flux.just(42).flatMap { step(n - 1) }
        step(depth).take(1).blockFirst()!!
    } catch (_: Throwable) { OVERFLOW }

    @Benchmark
    fun mutiny_deep_flatMap(): Int = try {
        fun step(n: Int): Multi<Int> =
            if (n <= 0) Multi.createFrom().item(42)
            else Multi.createFrom().item(42).flatMap { step(n - 1) }
        step(depth).select().first(1).collect().asList()
            .await().atMost(java.time.Duration.ofSeconds(5))
            .size
    } catch (_: Throwable) { OVERFLOW }

    @Suppress("UNCHECKED_CAST")
    @Benchmark
    fun monix_deep_flatMap(): Int = try {
        fun step(n: Int): MonixObservable<Any> =
            if (n <= 0) MonixObservable.now(42 as Any)
            else MonixObservable.now(42 as Any)
                .flatMap { v: Any -> step(n - 1) as MonixObservable<Any> }
        var value: Any? = null
        val latch = CountDownLatch(1)
        var err: Throwable? = null
        step(depth).toReactivePublisher<Any>(Scheduler.global()).subscribe(
            object : Subscriber<Any> {
                override fun onSubscribe(s: Subscription) = s.request(1)
                override fun onNext(t: Any) { value = t }
                override fun onError(t: Throwable) { err = t; latch.countDown() }
                override fun onComplete() { latch.countDown() }
            }
        )
        latch.await()
        err?.let { throw RuntimeException(it) }
        if (value != null) 1 else 0
    } catch (_: Throwable) { OVERFLOW }

    companion object {
        const val OVERFLOW = -1
    }
}
