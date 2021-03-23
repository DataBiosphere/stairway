package bio.terra.stairway.fixtures;

import bio.terra.stairway.FlightParameterDeserializer;
import bio.terra.stairway.FlightParameterSerializer;
import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.UUID;

public class FlightsTestNonPojo {

  private final UUID uuid;
  private final float value;

  public FlightsTestNonPojo(float value) {
    this.uuid = UUID.randomUUID();
    this.value = value;
  }

  private FlightsTestNonPojo(UUID uuid, float value) {
    this.uuid = uuid;
    this.value = value;
  }

  public UUID getUuid() {
    return uuid;
  }

  public float getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FlightsTestNonPojo nonPojo = (FlightsTestNonPojo) o;
    return Float.compare(nonPojo.value, value) == 0 && uuid.equals(nonPojo.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid, value);
  }

  public static FlightParameterSerializer<FlightsTestNonPojo> serializer() {
    return new SerializerDeserializer();
  }

  public static FlightParameterDeserializer<FlightsTestNonPojo> deserializer() {
    return new SerializerDeserializer();
  }

  public static class SerializerDeserializer
      implements FlightParameterDeserializer<FlightsTestNonPojo>,
          FlightParameterSerializer<FlightsTestNonPojo> {

    private static final int VERSION = 2;
    private final JsonFactory factory;
    private final JsonMapper mapper;

    public SerializerDeserializer() {
      factory = new JsonFactory();
      mapper = new JsonMapper();
    }

    @Override
    public String serialize(FlightsTestNonPojo nonPojo) {

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

        return new FlightsTestNonPojo(uuid, value);

      } catch (final IOException ex) {
        throw new JsonConversionException(ex.getCause());
      }
    }
  }
}
