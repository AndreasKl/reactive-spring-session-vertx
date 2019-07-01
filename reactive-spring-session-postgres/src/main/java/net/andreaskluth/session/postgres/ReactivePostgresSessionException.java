package net.andreaskluth.session.postgres;

public class ReactivePostgresSessionException extends RuntimeException {

  private static final long serialVersionUID = -7166595435862425393L;

  public ReactivePostgresSessionException(Throwable throwable) {
    super(throwable);
  }

  public ReactivePostgresSessionException(String message) {
    super(message);
  }

  public ReactivePostgresSessionException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
