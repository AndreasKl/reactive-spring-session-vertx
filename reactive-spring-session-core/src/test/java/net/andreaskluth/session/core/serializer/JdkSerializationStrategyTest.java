package net.andreaskluth.session.core.serializer;

class JdkSerializationStrategyTest extends TestSerializationStrategyBase {

  @Override
  SerializationStrategy strategy() {
    return new JdkSerializationStrategy();
  }
}
