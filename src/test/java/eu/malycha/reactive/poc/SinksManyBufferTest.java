package eu.malycha.reactive.poc;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Queue;

class SinksManyBufferTest {

    static int BUFFER_SIZE = 100;
    Sinks.Many<String> sink;
    Flux<String> flux;

    @BeforeEach
    void setUp() {
        sink = Sinks.many().multicast().onBackpressureBuffer(BUFFER_SIZE, false);
        flux = sink.asFlux();
    }

    @Test
    void sinkStartsAsWarm() {
        sink.tryEmitNext("Message 1");
        sink.tryEmitNext("Message 2");
        flux.subscribe(System.out::println);
        sink.tryEmitNext("Message 3");
    }

    @Test
    void sinkBecomesWarmIfSubscriberCancels() {
        flux.take(1).subscribe(System.out::println);
        sink.tryEmitNext("Message 1");
        sink.tryEmitNext("Message 2");
        flux.take(1).subscribe(System.out::println);
    }

    @Test
    void sinkIsHotIfSubscriberExists() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1).subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 2");
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeIsTooEarlyForSubscription() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeWithToProcessorIsFineForSubscription() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .toProcessor()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeWithCacheIsTooEarlyForSubscription() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .cache()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeWithShareIsTooEarlyForSubscription() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .share()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeWithSecondSinkIsFineForSubscription() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");

        Sinks.Many<String> buffer = Sinks.many().unicast().onBackpressureBuffer(new CircularFifoQueue<>(1));
        Disposable subscription = flux.subscribe(s -> {
            System.out.println("Buffer 1: " + s);
            buffer.tryEmitNext(s);
        });

        buffer.asFlux().take(1)
            .singleOrEmpty()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .doOnTerminate(subscription::dispose)
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
        sink.tryEmitNext("Message 4");
    }

    @Test
    void onSubscribeWithSecondSinkIsFineForSubscription_onlyLastIsBuffered() {
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");

        Sinks.Many<String> buffer = Sinks.many().unicast().onBackpressureBuffer(new CircularFifoQueue<>(1));
        Disposable subscription = flux.subscribe(s -> {
            System.out.println("Buffer 1: " + s);
            buffer.tryEmitNext(s);
        });

        sink.tryEmitNext("Message 1.1");
        sink.tryEmitNext("Message 1.2");
        sink.tryEmitNext("Message 1.3");

        buffer.asFlux().take(1)
            .singleOrEmpty()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .doOnTerminate(subscription::dispose)
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
        sink.tryEmitNext("Message 4");
    }

    // TODO: Filtering stale messages
}
