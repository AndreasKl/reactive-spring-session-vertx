package net.andreaskluth.session.postgres.serializer;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * {@link SerializationStrategy} using Jackson to serialize and deserialize data.
 *
 * <p><b>ATTENTION</b> This class uses Jacksons <code>objectMapper.enableDefaultTyping();</code>
 * feature. When unvalidated data is written to the session this could lead to code execution via so
 * called serialization gadgets.
 *
 * <p><b>ATTENTION</b> As jackson has more limitations, in what it can serialize and deserialize and
 * under which conditions, be super careful about the data stored in the session. Consider compiling
 * with {@code -parameters} otherwise constructors have to be annotated with {@link
 * com.fasterxml.jackson.annotation.JsonCreator}.
 *
 * <p>E.g. for maven
 *
 * <pre>{@code
 * <plugin>
 *   <groupId>org.apache.maven.plugins</groupId>
 *   <artifactId>maven-compiler-plugin</artifactId>
 *   <configuration>
 *     <compilerArgs>
 *       <arg>-verbose</arg>
 *       <arg>-parameters</arg>
 *       <arg>-Xlint:all</arg>
 *     </compilerArgs>
 *   </configuration>
 * </plugin>
 * }</pre>
 */
public class JacksonSerializationStrategy implements SerializationStrategy {

  private static final TypeReference<HashMap<String, Object>> stringObjectMapTypeRef =
      new TypeReference<>() {};
  private final ObjectMapper objectMapper;

  public JacksonSerializationStrategy() {
    this.objectMapper = defaultTypingObjectMapper();
  }

  private ObjectMapper defaultTypingObjectMapper() {
    ObjectMapper objectMapper = Jackson2ObjectMapperBuilder.json().build();
    objectMapper.enableDefaultTyping();
    objectMapper.registerModule(new ParameterNamesModule());
    return objectMapper;
  }

  @Override
  public Map<String, Object> deserialize(byte[] input) {
    requireNonNull(input, "input must not null");
    try {
      return objectMapper.readValue(input, stringObjectMapTypeRef);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  @Override
  public byte[] serialize(Map<String, Object> input) {
    requireNonNull(input, "input must not null");
    try {
      return objectMapper.writeValueAsBytes(input);
    } catch (JsonProcessingException e) {
      throw new SerializationException(e);
    }
  }
}
