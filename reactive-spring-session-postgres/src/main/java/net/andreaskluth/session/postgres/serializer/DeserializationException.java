package net.andreaskluth.session.postgres.serializer;

import net.andreaskluth.session.postgres.ReactivePostgresSessionException;

public class DeserializationException extends ReactivePostgresSessionException {

  private static final long serialVersionUID = 2260800020294378692L;

  public DeserializationException(Throwable throwable) {

    super(throwable);
  }
}
