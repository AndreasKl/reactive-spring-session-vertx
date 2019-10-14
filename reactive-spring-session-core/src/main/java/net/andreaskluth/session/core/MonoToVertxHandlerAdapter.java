package net.andreaskluth.session.core;

import static java.util.Objects.requireNonNull;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import reactor.core.publisher.MonoSink;

public class MonoToVertxHandlerAdapter<T> implements Handler<AsyncResult<T>> {

  private final MonoSink<T> sink;

  MonoToVertxHandlerAdapter(MonoSink<T> sink) {
    this.sink = requireNonNull(sink, "sink must not be null");
  }

  @Override
  public void handle(AsyncResult<T> event) {
    if (event.succeeded()) {
      sink.success(event.result());
      return;
    }
    sink.error(event.cause());
  }
}
