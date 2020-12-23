package bio.terra.stairway;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.openapitools.jackson.nullable.JsonNullableModule;

/** Common, singleton object mapper configured for Stairway use. */
class StairwayMapper {
  private static ObjectMapper objectMapper;

  static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper =
          new ObjectMapper()
              .registerModule(new ParameterNamesModule())
              .registerModule(new Jdk8Module())
              .registerModule(new JavaTimeModule())
              .registerModule(new JsonNullableModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              // TODO: replace with new method; the problem is we need to be promiscuous, because
              //  Stairway does not control what objects are serialized into the map.
              .enableDefaultTyping(ObjectMapper.DefaultTyping.EVERYTHING);
    }
    return objectMapper;
  }
}
