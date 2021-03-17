package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultFlightParameterSerializer implements FlightParameterSerializer {

  private ObjectMapper mapper;

  public DefaultFlightParameterSerializer(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String serialize(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert value to json string", ex);
    }
  }
}
