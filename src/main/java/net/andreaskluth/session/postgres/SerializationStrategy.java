package net.andreaskluth.session.postgres;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class SerializationStrategy {

  public byte[] serialize(Map<String, Object> sessionData) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(sessionData);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
