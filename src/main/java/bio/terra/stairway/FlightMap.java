package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * FlightMap wraps a {@code HashMap<String, Object>} It provides a subset of the HashMap methods. It
 * localizes code that casts from Object to the target type. It provides a way to set the map to be
 * immutable.
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
   * Temporary method to centralize the logic for resolving whether to use flightworking table data
   * or JSON column data; prefers flightworking data if available, but falls back to JSON.
   *
   * @param list List obtained from flightworking table; may be empty.
   * @param json JSON map obtained from a local column (flightlog::working_parameters or
   *     flight::output_parameters); may be null.
   * @return {@code Optional<FlightMap>} which will be populated with a FlightMap unless list is
   *     empty AND json is null.
   */
  @Deprecated
  public static Optional<FlightMap> create(List<FlightInput> list, @Nullable String json) {

    // An empty list can indicate either that (1) this flight was written with older code and does
    // not use flightworking, OR (2) the working map is empty.  In either case the JSON provide a
    // correct source of truth.

    if (list.isEmpty()) {
      if (json == null) {
        // Special case: null JSON string is expected in some output_parameters cases; in those
        // cases return an empty Optional.
        return Optional.empty();
      }
      FlightMap map = new FlightMap();
      map.fromJson(json);
      return Optional.of(map);
    } else {
      // Otherwise if we've a list with values, use it.
      return Optional.of(new FlightMap(list));
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
    String value = map.get(key);
    if (value == null) {
      return null;
    }

    try {
      return getObjectMapper().readValue(value, type);
    } catch (final JsonProcessingException ex) {
      throw new ClassCastException(
          "Found value '" + value + "' is not an instance of type " + type.getName());
    }
  }

  /**
   * Gets the raw (serialized) data stored for an Object in the hash map.
   *
   * @param key Key to lookup the data for in the map.
   * @return String mapped to key, or null if key does not exist.
   */
  public String getRaw(String key) {
    return map.get(key);
  }

  /**
   * Serialize an Object and place it into the map at a given key.
   *
   * @param key Key to store the object under.
   * @param value Object to serialize and store.
   * @throws JsonConversionException if object could not be serialized.
   */
  public void put(String key, Object value) {
    try {
      map.put(key, getObjectMapper().writeValueAsString(value));
    } catch (final JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert object to json string", ex);
    }
  }

  /**
   * Stores a raw (serialized) data value for a given key.
   *
   * @param key Key to store the data under.
   * @param value Data to store.
   */
  public void putRaw(String key, String value) {
    map.put(key, value);
  }

  public String toJson() {
    try {
      // Serializing as Map<String, RawJsonValue> will produce a format compatible with older code's
      // Map<String, Object> representation.
      Map<String, RawJsonValue> temporaryMap = new HashMap<>();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        temporaryMap.put(entry.getKey(), RawJsonValue.create(entry.getValue()));
      }
      return getObjectMapper().writeValueAsString(temporaryMap);
    } catch (final JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert map to json string", ex);
    }
  }

  public void fromJson(String json) {
    try {
      // Deserialize Map<String, Object> written by previous versions of code; deserializing as
      // Map<String, JsonNode> allows us to defer deserialization and obtain String representations
      // to store in our Map<String, String>.
      Map<String, JsonNode> legacyMap =
          getObjectMapper().readValue(json, new TypeReference<Map<String, JsonNode>>() {});
      for (Map.Entry<String, JsonNode> entry : legacyMap.entrySet()) {
        map.put(entry.getKey(), entry.getValue().toString());
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
