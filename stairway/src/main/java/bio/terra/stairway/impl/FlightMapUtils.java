package bio.terra.stairway.impl;

import static bio.terra.stairway.impl.StairwayMapper.getObjectMapper;

import bio.terra.stairway.FlightInput;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlightMapUtils provides methods to create a flight map from data pulled from the database and
 * format data from flight map to be stored in the database
 */
public class FlightMapUtils {
  /**
   * Create a flight map from an input list
   *
   * @param inputList input list form of the input parameters
   */
  static FlightMap makeFlightMap(List<FlightInput> inputList) {
    Map<String, Object> map = new HashMap<>();
    for (FlightInput input : inputList) {
      try {
        Object value = getObjectMapper().readValue(input.getValue(), Object.class);
        map.put(input.getKey(), value);
      } catch (IOException ex) {
        throw new JsonConversionException("Failed to convert json string to object", ex);
      }
    }
    return new FlightMap(map);
  }

  /**
   * Convert a flight map into the input list form. Used by the DAO to serialize the input
   * parameters.
   *
   * @return list of FlightInput
   */
  static List<FlightInput> makeFlightInputList(FlightMap flightMap) {
    Map<String, Object> map = flightMap.getMap();
    ArrayList<FlightInput> inputList = new ArrayList<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      try {
        String value = getObjectMapper().writeValueAsString(entry.getValue());
        inputList.add(new FlightInput(entry.getKey(), value));
      } catch (JsonProcessingException ex) {
        throw new JsonConversionException("Failed to convert value to json string", ex);
      }
    }
    return inputList;
  }

  /**
   * Depending on what version of code a Flight log was written with, we may have a JSON Map, a
   * {@code List<FlightInput>}, both, or neither at deserialization time. This method is used to
   * generate an {@code Optional<FlightMap>} based on what was contained in the database. This will
   * return an empty {@code Optional<FlightMap>} if json is null.
   *
   * @param inputList Entries in flightworking for a given Flight log. May be empty if Flight
   *     pre-existed flightworking table, or there are no working parameters.
   * @param json Map of working entries for a given Flight log in JSON. May be NULL.
   */
  static Optional<FlightMap> create(List<FlightInput> inputList, @Nullable String json) {

    // Note that currently if this is NULL, it indicates that the output_parameters field of the
    // flight table was empty.  Going forward (PF-703) we will expect NULL json to be the normal
    // case and should also look at flightInput.
    if (json == null) {
      return Optional.empty();
    }

    // TODO(PF-703): Once we stop writing JSON, we may still have JSON-only flight data in the
    // database, as well as flights with both. At that point we should favor inputList over json.

    FlightMap map = fromJson(json);

    // TODO(PF-703): Remove this block.
    if (!inputList.isEmpty()) {
      try {
        validateAgainst(map, inputList);
      } catch (Exception ex) {
        Logger logger = LoggerFactory.getLogger("FlightMap");
        logger.error("Input list does not match JSON: {}", ex.getMessage());
      }
    }

    return Optional.of(map);
  }

  /**
   * Validate whether the passed inputList will deserialize to the same set of keys and
   * corresponding types stored in the map. Note that this does not ensure the equality of values as
   * all types stored in FlightMap are not required to be comparable.
   *
   * <p>Throws {@link RuntimeException} if any key exists in inputList but not in map (or vice
   * versa). Throws {@link JsonProcessingException} if any value in inputList does not deserialize
   * to the type of its corresponding entry in the map.
   *
   * @param inputList
   */
  @VisibleForTesting
  static void validateAgainst(FlightMap flightMap, List<FlightInput> inputList) throws Exception {
    Map<String, Object> map = flightMap.getMap();
    if (inputList.size() != map.size())
      throw new RuntimeException(
          String.format(
              "Passed input list has %d entries, map has %d.", inputList.size(), map.size()));

    for (FlightInput input : inputList) {
      final String key = input.getKey();
      if (!map.containsKey(key)) {
        throw new RuntimeException(String.format("Key '%s' not found in map.", key));
      }
      final Object object = map.get(key);
      if (object != null) {
        getObjectMapper().readValue(input.getValue(), object.getClass());
      }
    }
  }

  @Deprecated
  static String toJson(FlightMap flightMap) {
    Map<String, Object> map = flightMap.getMap();
    try {
      return getObjectMapper().writeValueAsString(map);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert map to json string", ex);
    }
  }

  static FlightMap fromJson(String json) {
    try {
      Map<String, Object> map =
          getObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
      return new FlightMap(map);
    } catch (IOException ex) {
      throw new JsonConversionException("Failed to convert json string to map", ex);
    }
  }
}
