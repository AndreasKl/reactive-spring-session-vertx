package net.andreaskluth.session.core.serializer;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

abstract class TestSerializationStrategyBase {

  @Test
  void serializesAndDeserializesSimpleData() {
    Map<String, Object> testData = new HashMap<>();
    testData.put("ein", "wert");
    testData.put("noch", "ein wert");

    byte[] data = strategy().serialize(testData);
    Map<String, Object> dataMap = strategy().deserialize(data);

    assertThat(dataMap.get("ein")).isEqualTo("wert");
  }

  @Test
  void serializesAndDeserializesComplexObjects() {
    byte[] data = strategy().serialize(complexData());
    Map<String, Object> dataMap = strategy().deserialize(data);

    assertThat((Complex) dataMap.get("complex"))
        .extracting(Complex::getInstant)
        .isEqualTo(Instant.MIN);
    assertThat((Complex) dataMap.get("complex"))
        .extracting(Complex::getCreatedAt)
        .isEqualTo(LocalDateTime.MAX);
  }

  @Test
  void failsOnNotSerializableObjects() {
    assertThatThrownBy(
            () -> strategy().serialize(singletonMap("fails", new NotSerializable(Instant.now()))))
        .isInstanceOf(SerializationException.class);
  }

  @Test
  void failsOnNotDeserializableObjects() {
    assertThatThrownBy(() -> strategy().deserialize(new byte[] {23, 42}))
        .isInstanceOf(DeserializationException.class);
  }

  private Map<String, Object> complexData() {
    HashMap<String, Object> map = new HashMap<>();
    map.put("complex", new Complex(LocalDateTime.MAX, Instant.MIN));
    map.put("started", System.nanoTime());
    return map;
  }

  abstract SerializationStrategy strategy();

  @SuppressWarnings("UnusedVariable")
  private static class NotSerializable {

    private final Instant instant;

    private NotSerializable(Instant instant) {
      this.instant = instant;
    }
  }

  public static class Complex implements Serializable {

    private static final long serialVersionUID = 1;

    private final Instant instant;
    private final LocalDateTime createdAt;

    public Complex(LocalDateTime createdAt, Instant instant) {
      this.createdAt = createdAt;
      this.instant = instant;
    }

    public LocalDateTime getCreatedAt() {
      return createdAt;
    }

    public Instant getInstant() {
      return instant;
    }
  }
}
