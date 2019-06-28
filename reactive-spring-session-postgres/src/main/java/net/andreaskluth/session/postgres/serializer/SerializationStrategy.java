package net.andreaskluth.session.postgres.serializer;

import java.util.Map;

public interface SerializationStrategy {

  Map<String, Object> deserialize(byte[] binary);

  byte[] serialize(Map<String, Object> data);
}
