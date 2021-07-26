package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * FlightMap wraps a {@code HashMap<String, String>} It provides a map-like interface. It localizes
 * code that retrieves and deserializes from String to the target type, and serializes and stores to
 * String for storage. It provides a way to set the map to be immutable.
 */
public class FlightMap {
  private Map<String, String> map;

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
      map.put(input.getKey(), input.getValue());
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
    for (Map.Entry<String, String> entry : map.entrySet()) {
      inputList.add(new FlightInput(entry.getKey(), entry.getValue()));
    }
    return inputList;
  }

  /**
   * Depending on what version of code a Flight log was written with, we may have a JSON Map, a
   * {@code List<FlightInput>}, both, or neither at deserialization time. This method is used to
   * generate a FlightMap based on what was contained in the database.
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
      FlightMap map = new FlightMap();
      map.fromJson(json);
      return map;
    }

    // Otherwise just use the input list (even if it's empty).
    return new FlightMap(inputList);
  }

  /** Convert the map to an unmodifiable form. */
  public void makeImmutable() {
    map = Collections.unmodifiableMap(map);
  }

  /** Check map for emptiness */
  boolean isEmpty() {
    return map.isEmpty();
  }

  @Nullable
  public <T> T get(String key, TypeReference<T> typeReference) {
    String value = map.get(key);

    if (value == null) {
      return null;
    }

    try {
      return getObjectMapper().readValue(value, typeReference);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException(
          "Failed to deserialize value '"
              + value
              + "' from JSON to type "
              + typeReference.getType().getTypeName(),
          ex);
    }
  }

  /**
   * Return the object from the hash map deserialized to the right type. Throw an exception if the
   * Object cannot be deserialized to that type.
   *
   * @param <T> - type of class to expect in the hash map
   * @param key - key to lookup in the hash map
   * @param type - class requested
   * @return null if not found
   * @throws JsonConversionException if found, not deserializable to the requested type
   */
  @Nullable
  public <T> T get(String key, Class<T> type) {
    String value = map.get(key);

    if (value == null) {
      return null;
    }

    try {
      return getObjectMapper().readValue(value, type);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException(
          "Failed to deserialize value '" + value + "' from JSON to type " + type.getName(), ex);
    }
  }

  /**
   * Returns the raw String stored for a given key in the hash map.
   *
   * @param key to lookup in the hash map
   * @return null if not found
   */
  @Nullable
  public String getRaw(String key) {
    return map.get(key);
  }

  /**
   * Serialize the passed Object to a JSON String and store in the hash map
   *
   * @param key to store the data under
   * @param value to serialize and store
   * @throws JsonConversionException if object cannot be converted to JSON
   */
  public void put(String key, Object value) {
    try {
      map.put(key, getObjectMapper().writeValueAsString(value));
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert value to json string", ex);
    }
  }

  /**
   * Store a raw String representing a serialized object in the hash map
   *
   * @param key to store the String under
   * @param rawValue to store
   */
  public void putRaw(String key, String rawValue) {
    map.put(key, rawValue);
  }

  private void fromJson(String json) {
    try {
      Map<String, Object> legacyMap =
          getObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
      for (Map.Entry<String, Object> entry : legacyMap.entrySet()) {
        map.put(entry.getKey(), getObjectMapper().writeValueAsString(entry.getValue()));
      }
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
