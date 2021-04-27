package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlightMap wraps a {@code HashMap<String, Object>} It provides a subset of the HashMap methods. It
 * localizes code that casts from Object to the target type. It provides a way to set the map to be
 * immutable.
 */
public class FlightMap {
  private Map<String, Object> map;

  public FlightMap() {
    map = new HashMap<>();
  }

  /**
   * Alternate constructor, used by the DAO to re-create FlightMap from its serialized form.
   *
   * @param inputList input list form of the input parameters
   */
  FlightMap(List<FlightInput> inputList) {
    map = new HashMap<>();
    for (FlightInput input : inputList) {
      try {
        Object value = getObjectMapper().readValue(input.getValue(), Object.class);
        map.put(input.getKey(), value);
      } catch (IOException ex) {
        throw new JsonConversionException("Failed to convert json string to object", ex);
      }
    }
  }

  /**
   * Convert a flight map into the input list form. Used by the DAO to serialize the input
   * parameters.
   *
   * @return list of FlightInput
   */
  List<FlightInput> makeFlightInputList() {
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

    FlightMap map = new FlightMap();
    map.fromJson(json);

    // TODO(PF-703): Remove this block.
    if (!inputList.isEmpty()) {
      try {
        map.validateAgainst(inputList);
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
  void validateAgainst(List<FlightInput> inputList) throws Exception {

    if (inputList.size() != map.size())
      throw new RuntimeException(
          String.format(
              "Passed input list has %d entries, map has %d.", inputList.size(), map.size()));

    for (FlightInput input : inputList) {
      final String key = input.getKey();
      final Object object = map.get(key);
      if (object == null) {
        throw new RuntimeException(String.format("Key '%s' not found in map.", key));
      }
      getObjectMapper().readValue(input.getValue(), object.getClass());
    }
  }

  /** Convert the map to an unmodifiable form. */
  public void makeImmutable() {
    map = Collections.unmodifiableMap(map);
  }

  /**
   * Return the object from the hash map cast to the right type. Throw an exception if the Object
   * cannot be cast to that type.
   *
   * @param <T> - type of class to expect in the hash map
   * @param key - key to lookup in the hash map
   * @param type - class requested
   * @return null if not found
   * @throws ClassCastException if found, not castable to the requested type
   */
  public <T> T get(String key, Class<T> type) {
    Object value = map.get(key);
    if (value == null) {
      return null;
    }

    if (type.isInstance(value)) {
      return type.cast(value);
    }
    throw new ClassCastException(
        "Found value '" + value.toString() + "' is not an instance of type " + type.getName());
  }

  public void put(String key, Object value) {
    map.put(key, value);
  }

  @Deprecated
  public String toJson() {
    try {
      return getObjectMapper().writeValueAsString(map);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert map to json string", ex);
    }
  }

  public void fromJson(String json) {
    try {
      map = getObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
    } catch (IOException ex) {
      throw new JsonConversionException("Failed to convert json string to map", ex);
    }
  }

  // Truncate working map to only print first 500 characters
  @Override
  public String toString() {
    int truncateLength = 500;
    StringBuilder sb = new StringBuilder("{");
    sb.append(
        map.entrySet().stream()
            .map(
                entry -> {
                  String valString = String.valueOf(entry.getValue());
                  return entry.getKey()
                      + "="
                      + valString.substring(0, Math.min(truncateLength, valString.length()));
                })
            .collect(Collectors.joining(", ")));
    sb.append("}");
    return sb.toString();
  }
}
