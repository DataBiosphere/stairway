package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * This class implements default Jackson-based deserialization of classes and primitive types. It is
 * intended for internal use by FlightMap and FlightFilter classes when a custom deserializer is not
 * provided. It is not intended to be used directly.
 *
 * @param <T> Type that will be deserialized by this class.
 */
class DefaultFlightParameterDeserializer<T> implements FlightParameterDeserializer<T> {

  private final Class<T> type;
  private final ObjectMapper mapper;

  /**
   * Constructs a DefaultFlightParameterDeserializer<T> instance, which uses the passed Jackson
   * ObjectMapper to deserialize objects of type T to Strings.
   *
   * @param type - type Class<T>, required to pass to Jackson {@link ObjectMapper} at deserialization time
   * @param mapper - Jackson {@link ObjectMapper} used to serialize objects of type T in method
   *     {@link #deserialize(String)}
   */
  public DefaultFlightParameterDeserializer(Class<T> type, ObjectMapper mapper) {
    this.type = type;
    this.mapper = mapper;
  }

  @Override
  public T deserialize(String string) {
    try {
      return mapper.readValue(string, type);
    } catch (IOException ex) {
      throw new JsonConversionException("Failed to convert json string to object", ex);
    }
  }
}
