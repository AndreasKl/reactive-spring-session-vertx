package net.andreaskluth.session.core.serializer;

import net.andreaskluth.session.core.ReactiveSessionException;

public class SerializationException extends ReactiveSessionException {

  private static final long serialVersionUID = -5276484229664076436L;

  public SerializationException(Throwable throwable) {
    super(throwable);
  }
}
