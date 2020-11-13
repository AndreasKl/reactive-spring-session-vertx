package net.andreaskluth.session.core;

import io.vertx.core.Future;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class MonoToVertxHandlerAdapterTest {

  @Test
  void success() {
    Future<Boolean> booleanFuture = Future.succeededFuture(true);

    Mono.<Boolean>create(
            monoSink -> {
              MonoToVertxHandlerAdapter<Boolean> adapter =
                  new MonoToVertxHandlerAdapter<>(monoSink);
              adapter.handle(booleanFuture);
            })
        .as(StepVerifier::create)
        .expectNext(true)
        .verifyComplete();
  }

  @Test
  void failure() {
    Future<Boolean> booleanFuture = Future.failedFuture(new IllegalStateException());

    Mono.<Boolean>create(
            monoSink -> {
              MonoToVertxHandlerAdapter<Boolean> adapter =
                  new MonoToVertxHandlerAdapter<>(monoSink);
              adapter.handle(booleanFuture);
            })
        .as(StepVerifier::create)
        .verifyError();
  }
}
