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

import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations.*
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.CountDownLatch

/**
 * Monix 3.x benchmarks.
 *
 * Monix is a Scala library; Observable<A> is called here via Java interop.
 * Observable.range returns Observable<Object> (boxed Long) so casts are needed.
 * Blocking is done via toReactivePublisher to avoid Scala macro-gated
 * Task.runSyncUnsafe.
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class MonixBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    private val scheduler: Scheduler = Scheduler.global()

    /** Blocking collect via Reactive Streams subscriber. */
    @Suppress("UNCHECKED_CAST")
    private fun blockingList(obs: Observable<*>): List<Any> {
        val result = ArrayList<Any>()
        val latch = CountDownLatch(1)
        var err: Throwable? = null
        val publisher = obs.toReactivePublisher<Any>(scheduler)
        publisher.subscribe(object : Subscriber<Any> {
            override fun onSubscribe(s: Subscription) = s.request(Long.MAX_VALUE)
            override fun onNext(t: Any) { result.add(t) }
            override fun onError(t: Throwable) { err = t; latch.countDown() }
            override fun onComplete() { latch.countDown() }
        })
        latch.await()
        err?.let { throw RuntimeException(it) }
        return result
    }

    @Benchmark
    fun baseline_toList(): Int =
        blockingList(Observable.range(0L, size.toLong(), 1L)).size

    @Benchmark
    fun map_toList(): Int =
        blockingList(
            Observable.range(0L, size.toLong(), 1L)
                .map { v -> (v as Long) * 2L }
        ).size

    @Benchmark
    fun filter_toList(): Int =
        blockingList(
            Observable.range(0L, size.toLong(), 1L)
                .filter { v -> (v as Long) % 2L == 0L }
        ).size

    @Benchmark
    fun take_toList(): Int =
        blockingList(
            Observable.range(0L, size.toLong(), 1L)
                .take(size.toLong() / 2)
        ).size

    @Benchmark
    fun chain_map_filter_take(): Int =
        blockingList(
            Observable.range(0L, size.toLong(), 1L)
                .map { v -> (v as Long) * 2L }
                .filter { v -> (v as Long) % 4L == 0L }
                .take(size.toLong() / 4)
        ).size

    @Benchmark
    fun concatMap_toList(): Int =
        blockingList(
            Observable.range(0L, (size / 10).toLong(), 1L)
                .concatMap { v ->
                    val i = v as Long
                    Observable.range(i, i + 3L, 1L)
                }
        ).size

    @Benchmark
    fun flatMapSequential_toList(): Int =
        blockingList(
            Observable.range(0L, (size / 10).toLong(), 1L)
                .concatMap { v ->
                    val i = v as Long
                    Observable.range(i, i + 3L, 1L)
                }
        ).size

    @Benchmark
    fun flatMap_toList(): Int =
        blockingList(
            Observable.range(0L, (size / 10).toLong(), 1L)
                .flatMap { v ->
                    val i = v as Long
                    Observable.range(i, i + 3L, 1L)
                }
        ).size

    @Benchmark
    fun fold_sum(): Long =
        blockingList(
            Observable.range(0L, size.toLong(), 1L)
                .foldLeft({ 0L }, { acc: Long, v: Any -> acc + (v as Long) })
        ).first() as Long
}
