package bio.terra.stairway;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.annotations.VisibleForTesting;
import org.openapitools.jackson.nullable.JsonNullableModule;

/** Common, singleton object mapper configured for Stairway use. */
@VisibleForTesting
public class StairwayMapper {
  private static ObjectMapper objectMapper;

  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      // Create a permissive type validator for backward compatibility
      BasicPolymorphicTypeValidator typeValidator =
          BasicPolymorphicTypeValidator.builder().allowIfSubType(Object.class).build();

      objectMapper =
          new ObjectMapper()
              .registerModule(new ParameterNamesModule())
              .registerModule(new Jdk8Module())
              .registerModule(new JavaTimeModule())
              .registerModule(new JsonNullableModule())
              .registerModule(new GuavaModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              // Replace deprecated enableDefaultTyping with modern approach
              .activateDefaultTyping(typeValidator, ObjectMapper.DefaultTyping.NON_FINAL);
    }
    return objectMapper;
  }
}
