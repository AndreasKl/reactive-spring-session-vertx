package net.andreaskluth.session.postgres.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

public class JdkSerializationSerializationStrategy implements SerializationStrategy {

  public static final int DEFAULT_OUTPUT_STREAM_SIZE = 128;

  public Map<String, Object> deserialize(byte[] binary) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binary);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      @SuppressWarnings("unchecked")
      var dataMap = (Map<String, Object>) ois.readObject();
      return dataMap;
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] serialize(Map<String, Object> sessionData) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(DEFAULT_OUTPUT_STREAM_SIZE);
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(sessionData);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
