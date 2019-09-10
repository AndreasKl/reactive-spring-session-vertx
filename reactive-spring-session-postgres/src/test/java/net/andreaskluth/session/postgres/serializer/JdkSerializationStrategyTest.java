package net.andreaskluth.session.postgres.serializer;

class JdkSerializationStrategyTest extends TestSerializationStrategyBase {

  @Override
  SerializationStrategy strategy() {
    return new JdkSerializationStrategy();
  }
}
