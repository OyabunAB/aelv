package se.oyabun.aelv.tck

import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.testng.SkipException
import org.testng.annotations.Test
import se.oyabun.aelv.One
import se.oyabun.aelv.UpstreamErrorException

@Test
class OneVerification : PublisherVerification<Int>(TestEnvironment()) {

    override fun createPublisher(elements: Long): Publisher<Int> = when (elements) {
        0L -> One.error(UpstreamErrorException(RuntimeException("empty")))
        1L -> One.of(1)
        else -> throw SkipException("One<T> emits exactly 1 element, cannot produce $elements")
    }

    override fun createFailedPublisher(): Publisher<Int> = Publisher { subscriber ->
        subscriber.onSubscribe(object : org.reactivestreams.Subscription {
            override fun request(n: Long) {}
            override fun cancel() {}
        })
        subscriber.onError(UpstreamErrorException(RuntimeException("tck failed publisher")))
    }
}
