package se.oyabun.aelv.tck

import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.annotations.Test
import se.oyabun.aelv.Many
import se.oyabun.aelv.UpstreamErrorException

@Test
class ManyVerification : PublisherVerification<Int>(TestEnvironment()) {

    override fun createPublisher(elements: Long): Publisher<Int> =
        Many.of(generateSequence(0L) { if (it + 1 < elements) it + 1 else null }.map { it.toInt() }.asIterable())

    override fun createFailedPublisher(): Publisher<Int> = Publisher { subscriber ->
        subscriber.onSubscribe(object : org.reactivestreams.Subscription {
            override fun request(n: Long) {}
            override fun cancel() {}
        })
        subscriber.onError(UpstreamErrorException(RuntimeException("tck failed publisher")))
    }
}
