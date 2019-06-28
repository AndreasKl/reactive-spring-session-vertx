package net.andreaskluth.session.postgres.serializer;

public class JdkSerializationStrategyTest extends TestSerializationStrategyBase {

  @Override
  SerializationStrategy strategy() {
    return new JdkSerializationStrategy();
  }
}
