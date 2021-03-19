package bio.terra.stairway.fixtures;

import bio.terra.stairway.FlightParameterDeserializer;
import bio.terra.stairway.FlightParameterSerializer;
import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

@AutoValue
public abstract class FlightsTestNonPojo {
  public abstract UUID getUuid();

  public abstract float getValue();

  public static FlightsTestNonPojo create(float value) {
    return new AutoValue_FlightsTestNonPojo(UUID.randomUUID(), value);
  }

  private static FlightsTestNonPojo create(UUID uuid, float value) {
    return new AutoValue_FlightsTestNonPojo(uuid, value);
  }

  public static FlightParameterSerializer serializer() {
    return new SerializerDeserializer();
  }

  public static FlightParameterDeserializer<FlightsTestNonPojo> deserializer() {
    return new SerializerDeserializer();
  }

  public static class SerializerDeserializer extends FlightParameterDeserializer<FlightsTestNonPojo>
      implements FlightParameterSerializer {

    private static final int VERSION = 2;
    private final JsonFactory factory;
    private final JsonMapper mapper;

    public SerializerDeserializer() {
      super(FlightsTestNonPojo.class);
      factory = new JsonFactory();
      mapper = new JsonMapper();
    }

    @Override
    public String serialize(Object object) {

      FlightsTestNonPojo nonPojo = safeCast(object);
      try {
        OutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator generator = factory.createGenerator(outputStream);

        generator.writeStartObject();
        generator.writeNumberField("version", 2);
        generator.writeStringField("uuid", nonPojo.getUuid().toString());
        generator.writeNumberField("value", nonPojo.getValue());
        generator.writeEndObject();
        generator.close();
        outputStream.close();
        return outputStream.toString();
      } catch (final IOException ex) {
        throw new JsonConversionException(ex.getCause());
      }
    }

    @Override
    public FlightsTestNonPojo deserialize(String string) {

      try {
        JsonNode map = mapper.readTree(string);

        int version = map.has("version") ? map.get("version").asInt() : 1;
        UUID uuid = UUID.fromString(map.get("uuid").textValue());

        float value;
        if (version < 2) {
          int numerator = map.get("value_numerator").asInt();
          int denominator = map.get("value_denominator").asInt();
          value = (float) numerator / (float) denominator;
        } else {
          value = map.get("value").floatValue();
        }

        return FlightsTestNonPojo.create(uuid, value);

      } catch (final IOException ex) {
        throw new JsonConversionException(ex.getCause());
      }
    }
  }
}
