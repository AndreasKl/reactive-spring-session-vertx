package net.andreaskluth.session.core;

public class ReactiveSessionException extends RuntimeException {

  private static final long serialVersionUID = -7166595435862425393L;

  public ReactiveSessionException(Throwable throwable) {
    super(throwable);
  }

  public ReactiveSessionException(String message) {
    super(message);
  }

  public ReactiveSessionException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
