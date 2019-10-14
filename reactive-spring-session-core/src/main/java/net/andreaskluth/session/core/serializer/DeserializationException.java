package net.andreaskluth.session.core.serializer;

import net.andreaskluth.session.core.ReactiveSessionException;

public class DeserializationException extends ReactiveSessionException {

  private static final long serialVersionUID = 2260800020294378692L;

  public DeserializationException(Throwable throwable) {

    super(throwable);
  }
}
