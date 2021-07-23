package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import java.util.HashMap;
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

  /** Construct an empty flight map */
  public FlightMap() {
    map = new HashMap<>();
  }

  /**
   * This constructor is used when Stairway re-creates the flight map from the database. It is not
   * intended for client use.
   *
   * @param map deserialized input map from database
   */
  public FlightMap(Map<String, String> map) {
    this.map = map;
  }

  /**
   * Accessor for the contained map. Used for storing the map into the database. It is not intended
   * for client use.
   *
   * @return contained map
   */
  public Map<String, String> getMap() {
    return map;
  }

  /** Convert the map to an unmodifiable form. */
  public void makeImmutable() {
    map = Collections.unmodifiableMap(map);
  }

  /** Check map for emptiness */
  public boolean isEmpty() {
    return map.isEmpty();
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
