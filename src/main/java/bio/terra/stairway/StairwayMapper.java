package bio.terra.stairway;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.openapitools.jackson.nullable.JsonNullableModule;

/** Common, singleton object mapper configured for Stairway use. */
@VisibleForTesting
public class StairwayMapper {
  private static ObjectMapper objectMapper;

  /**
   * Extend JsonSerializer for type RawJsonValue to perform custom serialization of raw JSON values
   * stored as Strings. Given we know that the Strings we have stored have type info encoded within
   * them, we want to override the serializeWithType method to output the raw string.
   */
  private static final class RawJsonValueSerializer extends JsonSerializer<RawJsonValue> {
    @Override
    public void serializeWithType(
        RawJsonValue value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      serialize(value, gen, serializers);
    }

    @Override
    public void serialize(RawJsonValue value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeRawValue(value.getJsonValue());
    }
  }

  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {

      // Hook serialization of type RawJsonValue with our custom serializer.
      SimpleModule rawJsonModule = new SimpleModule();
      rawJsonModule.addSerializer(RawJsonValue.class, new RawJsonValueSerializer());

      objectMapper =
          new ObjectMapper()
              // Register the module containing our RawJsonValueSerializer instance.
              .registerModule(rawJsonModule)
              .registerModule(new ParameterNamesModule())
              .registerModule(new Jdk8Module())
              .registerModule(new JavaTimeModule())
              .registerModule(new JsonNullableModule())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              // TODO: replace with new method; the problem is we need to be promiscuous, because
              //  Stairway does not control what objects are serialized into the map.
              .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    }
    return objectMapper;
  }
}
