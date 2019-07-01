package net.andreaskluth.session.postgres.serializer;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

public class JdkSerializationStrategy implements SerializationStrategy {

  public static final int DEFAULT_OUTPUT_STREAM_SIZE = 128;

  public Map<String, Object> deserialize(byte[] input) {
    requireNonNull(input, "input must not null");
    try (ByteArrayInputStream bais = new ByteArrayInputStream(input);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      @SuppressWarnings("unchecked")
      var dataMap = (Map<String, Object>) ois.readObject();
      return dataMap;
    } catch (IOException | ClassNotFoundException e) {
      throw new DeserializationException(e);
    }
  }

  public byte[] serialize(Map<String, Object> input) {
    requireNonNull(input, "input must not null");
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(DEFAULT_OUTPUT_STREAM_SIZE);
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(input);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }
}
