package net.andreaskluth.session.postgres.serializer;

class JacksonSerializationStrategyTest extends TestSerializationStrategyBase {

  @Override
  SerializationStrategy strategy() {
    return new JacksonSerializationStrategy();
  }
}
