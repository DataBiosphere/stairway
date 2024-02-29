package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.lang.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Accessor for the contained map. Used for storing the map into the database. It is not intended
   * for client use.
   *
   * @return contained map
   */
  public Map<String, String> getMap() {
    return Collections.unmodifiableMap(map);
  }

  /** Convert the map to an unmodifiable form. */
  public void makeImmutable() {
    map = Collections.unmodifiableMap(map);
  }

  /**
   * Check map for emptiness
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return map.isEmpty();
  }

  /**
   * Returns true if this map contains a mapping of the passed key.
   *
   * @param key to look up in the map
   * @return true if present
   */
  public boolean containsKey(String key) {
    return map.containsKey(key);
  }

  /**
   * Return the object from the flight map deserialized to the right non-parameterized type. Throw
   * an exception if the Object cannot be deserialized to that type. For parameterized types, the
   * {@code TypeReference<T>} overload of this method offers more type safety and should be
   * preferred.
   *
   * @param <T> - type of class to expect in the flight map
   * @param key - key to lookup in the flight map
   * @param type - class requested
   * @return null if not found or if a null value is stored at that key (use method {@code
   *     containsKey()} to differentiate)
   * @throws JsonConversionException if not deserializable to the requested type
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
   * Return the object from the flight map deserialized to the right type. Throw an exception if the
   * Object cannot be deserialized to that type. This overload is preferred when deserializing
   * parameterized types as it provides stronger type checking at deserialization time.
   *
   * @param <T> - type of class to expect in the flight map
   * @param key - key to lookup in the flight map
   * @param typeReference - class requested
   * @return null if not found or if a null value is stored at that key (use method {@code
   *     containsKey()} to differentiate)
   * @throws JsonConversionException if not deserializable to the requested type
   */
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
   * Returns the raw String stored for a given key in the flight map.
   *
   * @param key to lookup in the flight map
   * @return null if not found
   */
  @Nullable
  public String getRaw(String key) {
    return map.get(key);
  }

  /**
   * Serialize the passed Object to a JSON String and store in the flight map
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
   * Store a raw String representing a serialized object in the flight map
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
