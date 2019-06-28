package net.andreaskluth.session.postgres.serializer;

public class JacksonSerializationStrategyTest extends TestSerializationStrategyBase {

  @Override
  SerializationStrategy strategy() {
    return new JacksonSerializationStrategy();
  }
}
