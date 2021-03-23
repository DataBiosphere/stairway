package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class implements default Jackson-based serialization of classes and primitive types. It is
 * intended for internal use by FlightMap and FlightFilter classes when a custom serializer is not
 * provided. It is not intended to be used directly.
 *
 * @param <T> Type that will be serialized by this class.
 */
class DefaultFlightParameterSerializer<T> implements FlightParameterSerializer<T> {

  private final ObjectMapper mapper;

  /**
   * Constructs a DefaultFlightParameterSerializer<T> instance, which uses the passed Jackson
   * ObjectMapper to serialize objects of type T to Strings.
   *
   * @param mapper - Jackson {@link ObjectMapper} used to serialize objects of type T in method
   *     {@link #serialize(T)}
   */
  public DefaultFlightParameterSerializer(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String serialize(T object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert value to json string", ex);
    }
  }
}
