package bio.terra.stairway.impl;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.FlightInput;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    FlightMap flightMap = new FlightMap();
    fillInFlightMap(flightMap, inputList);
    return flightMap;
  }

  /**
   * Fill in a flight map from an input list
   *
   * @param flightMap incoming flight map
   * @param inputList input list form of the input parameters
   */
  static void fillInFlightMap(FlightMap flightMap, List<FlightInput> inputList) {
    for (FlightInput input : inputList) {
      flightMap.putRaw(input.getKey(), input.getValue());
    }
  }

  /**
   * Convert a flight map into the input list form. Used by the DAO to serialize the input
   * parameters.
   *
   * @return list of FlightInput
   */
  static List<FlightInput> makeFlightInputList(FlightMap flightMap) {
    Map<String, String> map = flightMap.getMap();
    ArrayList<FlightInput> inputList = new ArrayList<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      inputList.add(new FlightInput(entry.getKey(), entry.getValue()));
    }
    return inputList;
  }

  /**
   * Depending on what version of code a Flight log was written with, we may have a JSON Map, a
   * {@code List<FlightInput>}, both, or neither at deserialization time. This method is used to
   * generate FlightMap> based on what was contained in the database.
   *
   * <p>Flights written with:
   *
   * <ul>
   *   <li>Version < 0.0.50: Only json is valid, inputList is always empty.
   *   <li>Version 0.0.50 - 0.0.60: json and inputList both valid and must be consistent with one
   *       another.
   *   <li>Version >= 0.0.61: json always null, inputList is always valid (possibly empty).
   * </ul>
   *
   * @param inputList Entries in flightworking for a given Flight log. May be empty if Flight
   *     pre-existed flightworking table, or there are no working parameters.
   * @param json Map of working entries for a given Flight log in JSON. May be NULL.
   */
  static FlightMap create(List<FlightInput> inputList, @Nullable String json) {
    // If we have an empty list AND there is valid JSON, this predates the flightworking table...
    // use the json in this case.
    if (inputList.isEmpty() && json != null) {
      return fromJson(json);
    }

    // Otherwise just use the input list (even if it's empty).
    return makeFlightMap(inputList);
  }

  static FlightMap fromJson(String json) {
    try {
      Map<String, Object> legacyMap =
          getObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
      FlightMap flightMap = new FlightMap();
      for (Map.Entry<String, Object> entry : legacyMap.entrySet()) {
        flightMap.putRaw(entry.getKey(), getObjectMapper().writeValueAsString(entry.getValue()));
      }
      return flightMap;
    } catch (IOException ex) {
      throw new JsonConversionException("Failed to convert json string to map", ex);
    }
  }
}
