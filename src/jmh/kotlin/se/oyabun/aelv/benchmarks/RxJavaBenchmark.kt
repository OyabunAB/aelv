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

import io.reactivex.rxjava3.core.Observable
import org.openjdk.jmh.annotations.*

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class RxJavaBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    @Benchmark
    fun baseline_toList(): Int =
        Observable.range(0, size).toList().blockingGet().size

    @Benchmark
    fun map_toList(): Int =
        Observable.range(0, size).map { it * 2 }.toList().blockingGet().size

    @Benchmark
    fun filter_toList(): Int =
        Observable.range(0, size).filter { it % 2 == 0 }.toList().blockingGet().size

    @Benchmark
    fun take_toList(): Int =
        Observable.range(0, size).take(size.toLong() / 2).toList().blockingGet().size

    @Benchmark
    fun chain_map_filter_take(): Int =
        Observable.range(0, size)
            .map { it * 2 }
            .filter { it % 4 == 0 }
            .take(size.toLong() / 4)
            .toList().blockingGet().size

    @Benchmark
    fun concatMap_toList(): Int =
        Observable.range(0, size / 10)
            .concatMap { i -> Observable.just(i, i + 1, i + 2) }
            .toList().blockingGet().size

    @Benchmark
    fun flatMap_toList(): Int =
        Observable.range(0, size / 10)
            .flatMap { i -> Observable.just(i, i + 1, i + 2) }
            .toList().blockingGet().size

    private fun ioWork(i: Int): Observable<Int> =
        io.reactivex.rxjava3.core.Observable.timer(Workload.IO_DELAY_MS, java.util.concurrent.TimeUnit.MILLISECONDS, io.reactivex.rxjava3.schedulers.Schedulers.computation())
            .flatMap { Observable.range(i, Workload.ITEMS_PER_CALL) }

    @Benchmark
    fun flatMap_io(): Int =
        Observable.range(0, size / 10)
            .flatMap { i -> ioWork(i) }
            .toList().blockingGet().size

    @Benchmark
    fun concatMap_io(): Int =
        Observable.range(0, size / 10)
            .concatMap { i -> ioWork(i) }
            .toList().blockingGet().size

    @Benchmark
    fun fold_sum(): Long =
        Observable.range(0, size)
            .reduce(0L) { acc, i -> acc + i }
            .blockingGet()!!
}
