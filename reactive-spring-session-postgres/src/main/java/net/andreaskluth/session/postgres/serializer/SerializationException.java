package net.andreaskluth.session.postgres.serializer;

import net.andreaskluth.session.postgres.ReactivePostgresSessionException;

public class SerializationException extends ReactivePostgresSessionException {

  private static final long serialVersionUID = -5276484229664076436L;

  public SerializationException(Throwable throwable) {
    super(throwable);
  }
}
