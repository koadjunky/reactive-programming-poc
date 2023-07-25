package eu.malycha.reactive.poc;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
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
        // Test checks if sink is warm before subscribing - it means it should buffer message 1 and 2 and emit them after
        // subscribe. Then it becomes hot and emits Message 3.
        sink.tryEmitNext("Message 1");
        sink.tryEmitNext("Message 2");
        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("Message 1")
            .expectNext("Message 2")
            .then(() -> sink.tryEmitNext("Message 3"))
            .expectNext("Message 3")
            .thenCancel()
            .verify();
    }

    @Test
    void sinkBecomesWarmIfSubscriberCancels() {
        // Test checks if sink returns to warm state after subscriber disconnects. It emits:
        //  * Message 1 in hot state
        //  * Bufferes message 2 (so it returns to warm state - in hot state, it would lose the message)
        //  * Emits message 2 after second subscriber connects
        StepVerifier.create(flux.take(1))
            .expectSubscription()
            .then(() -> sink.tryEmitNext("Message 1"))
            .expectNext("Message 1")
            .then(() -> sink.tryEmitNext("Message 2"))
            .expectComplete()
            .verify();

        StepVerifier.create(flux.take(1))
            .expectSubscription()
            .expectNext("Message 2")
            .expectComplete()
            .verify();
    }

    @Test
    void sinkIsHotIfSubscriberExists() {
        // Test checks if sink is hot when subscriber exists. Since first subscriber consumes message 1, it will
        // not wait for second subscriber. Second subscriber sees only message 2 emitted after it subscribes.
        StepVerifier subscriber1 = StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("Message 1")
            .expectNext("Message 2")
            .expectNext("Message 3")
            .thenCancel()
            .verifyLater();

        sink.tryEmitNext("Message 1");

        StepVerifier.create(flux.take(1))
            .expectSubscription()
            .then(() -> sink.tryEmitNext("Message 2"))
            .expectNext("Message 2")
            .expectComplete()
            .verify(Duration.ofSeconds(1));

        sink.tryEmitNext("Message 3");
        subscriber1.verify(Duration.ofSeconds(1));
    }

    @Test
    void onSubscribeIsTooEarlyForSubscription() {
        // Test proves that message emitted in doOnSubscribe is not captured by subscribe on hot producer.
        // Second subscriber only sees message 3 emitting after subscribe.
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .doOnSubscribe(s -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void doFirstIsTooEarlyForSubscription() {
        // Test proves that message emitted in doFirst is not captured by subscribe on hot producer.
        // Second subscriber only sees message 3 emitting after subscribe.
        flux.subscribe(s -> System.out.println("Subscriber 1: " + s));
        sink.tryEmitNext("Message 1");
        flux.take(1)
            .singleOrEmpty()
            .doFirst(() -> sink.tryEmitNext("Message 2"))
            .subscribe(s -> System.out.println("Subscriber 2: " + s));
        sink.tryEmitNext("Message 3");
    }

    @Test
    void onSubscribeWithToProcessorIsFineForSubscription() {
        // Test proves that message emitted in doOnSubscribe is captured by subscribe on hot producer
        // if ran through (deprecated) processor.
        // Second subscriber sees message 2 emitted during subscribe.
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
        // Test proves that message emitted in doOnSubscribe is not captured by subscribe on hot producer when ran through cache().
        // Second subscriber only sees message 3 emitting after subscribe.
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
        // Test proves that message emitted in doOnSubscribe is not captured by subscribe on hot producer when ran through share().
        // Second subscriber only sees message 3 emitting after subscribe.
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
        // Test proves that message emitted in doOnSubscribe is captured by subscribe on hot producer
        // if ran through Sinks.many().
        // Second subscriber sees message 2 emitted during subscribe.
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
        // Test proves that message emitted in doOnSubscribe is captured by subscribe on warm producer
        // if ran through Sinks.many().
        // Second subscriber sees message 2 emitted during subscribe. Messages collected before subscribe
        // are ignored.
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
