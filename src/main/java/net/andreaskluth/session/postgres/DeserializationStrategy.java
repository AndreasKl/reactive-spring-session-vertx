package net.andreaskluth.session.postgres;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class DeserializationStrategy {

  @SuppressWarnings("unchecked")
  public Map<String, Object> deserialize(byte[] binary) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binary);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (Map<String, Object>) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
