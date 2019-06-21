package net.andreaskluth.net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.Test;

public class SerializationStrategyTest {

  @Test
  public void serializesAndDeserializes() {
    SerializationStrategy serializationStrategy = new SerializationStrategy();
    DeserializationStrategy deserializationStrategy = new DeserializationStrategy();

    byte[] serialized = serializationStrategy.serialize(Map.of("test", "data"));
    Map<String, Object> deserialized = deserializationStrategy.deserialize(serialized);

    assertThat(deserialized.get("test")).isEqualTo("data");
  }
}
