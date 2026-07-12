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
package se.oyabun.aelv.tck

import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.Test
import se.oyabun.aelv.Many

@Test
class ManyVerification : PublisherVerification<Int>(TestEnvironment(3000L, 500L)) {

    override fun createPublisher(elements: Long): Publisher<Int> =
        Many.from(generateSequence(0L) { if (it + 1 < elements) it + 1 else null }.map { it.toInt() }.asIterable())

    override fun createFailedPublisher(): Publisher<Int> = Publisher { subscriber ->
        subscriber.onSubscribe(object : org.reactivestreams.Subscription {
            override fun request(n: Long) {}
            override fun cancel() {}
        })
        subscriber.onError(RuntimeException("tck failed publisher"))
    }
}
