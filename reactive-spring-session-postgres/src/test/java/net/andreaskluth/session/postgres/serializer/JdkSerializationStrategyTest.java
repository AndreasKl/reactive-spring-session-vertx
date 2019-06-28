package net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import net.andreaskluth.session.postgres.serializer.JdkSerializationStrategy;
import org.junit.Test;

public class JdkSerializationJdkSerializationStrategyTest {

  @Test
  public void serializesAndDeserializes() {
    var serializationStrategy = new JdkSerializationStrategy();

    var serialized = serializationStrategy.serialize(Map.of("test", "data"));
    var deserialized = serializationStrategy.deserialize(serialized);

    assertThat(deserialized.get("test")).isEqualTo("data");
  }
}
