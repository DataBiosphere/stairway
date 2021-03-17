package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class DefaultFlightParameterDeserializer<T> extends FlightParameterDeserializer<T> {

  private ObjectMapper mapper;

  public DefaultFlightParameterDeserializer(Class<T> type, ObjectMapper mapper) {
    super(type);
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
