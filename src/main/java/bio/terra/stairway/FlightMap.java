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
import java.util.stream.Collectors;

/**
 * FlightMap wraps a {@code HashMap<String, Object>} It provides a subset of the HashMap methods. It
 * localizes code that casts from Object to the target type. It provides a way to set the map to be
 * immutable.
 */
public class FlightMap {
  private Map<String, Object> map;
  private FlightMapUpgrader upgrader;

  public FlightMap() {
    map = new HashMap<>();
    upgrader = null;
  }

  public FlightMap(FlightMapUpgrader upgrader) {
    this();
    this.upgrader = upgrader;
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

  public String toJson() {
    try {
      return getObjectMapper().writeValueAsString(map);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert map to json string", ex);
    }
  }

  private String upgrade(String json) {
    if (upgrader == null) return json;

    try {
      JsonNode root = getObjectMapper().readTree(json);
      upgrader.upgrade(new FlightMapUpgradeView(root));
      return getObjectMapper().writeValueAsString(root);
    } catch (final JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert json string to map during upgrade", ex);
    }
  }

  public void fromJson(String json) {
    try {
      json = upgrade(json);
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
