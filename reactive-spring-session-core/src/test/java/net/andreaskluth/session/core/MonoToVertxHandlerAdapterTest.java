package net.andreaskluth.session.core;

import io.vertx.core.impl.FutureFactoryImpl;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class MonoToVertxHandlerAdapterTest {

  @Test
  void success() {
    var booleanFuture = new FutureFactoryImpl().succeededFuture(true);

    Mono.<Boolean>create(
            monoSink -> {
              var adapter = new MonoToVertxHandlerAdapter<>(monoSink);
              adapter.handle(booleanFuture);
            })
        .as(StepVerifier::create)
        .expectNext(true)
        .verifyComplete();
  }

  @Test
  void failure() {
    var booleanFuture = new FutureFactoryImpl().<Boolean>failedFuture(new IllegalStateException());

    Mono.<Boolean>create(
            monoSink -> {
              var adapter = new MonoToVertxHandlerAdapter<>(monoSink);
              adapter.handle(booleanFuture);
            })
        .as(StepVerifier::create)
        .verifyError();
  }
}
