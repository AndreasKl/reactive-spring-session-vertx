package net.andreaskluth.session.core.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;

abstract class TestSerializationStrategyBase {

  @Test
  void serializesAndDeserializesSimpleData() {
    var data = strategy().serialize(Map.of("ein", "wert", "noch", "ein wert"));
    var dataMap = strategy().deserialize(data);

    assertThat(dataMap.get("ein")).isEqualTo("wert");
  }

  @Test
  void serializesAndDeserializesComplexObjects() {
    var data = strategy().serialize(complexData());
    var dataMap = strategy().deserialize(data);

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
            () -> strategy().serialize(Map.of("fails", new NotSerializable(Instant.now()))))
        .isInstanceOf(SerializationException.class);
  }

  @Test
  void failsOnNotDeserializableObjects() {
    assertThatThrownBy(() -> strategy().deserialize(new byte[] {23, 42}))
        .isInstanceOf(DeserializationException.class);
  }

  private Map<String, Object> complexData() {
    return Map.of(
        "complex", new Complex(LocalDateTime.MAX, Instant.MIN), "started", System.nanoTime());
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
