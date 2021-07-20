package bio.terra.stairway;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * FlightMap wraps a {@code HashMap<String, Object>} It provides a subset of the HashMap methods. It
 * localizes code that casts from Object to the target type. It provides a way to set the map to be
 * immutable.
 */
public class FlightMap {
  private Map<String, Object> map;

  /** Construct an empty flight map */
  public FlightMap() {
    map = new HashMap<>();
  }

  /**
   * This constructor is used when Stairway re-creates the flight map from the database. It is not
   * intended for client use.
   * @param map deserialized input map from database
   */
  public FlightMap(Map<String, Object> map) {
    this.map = map;
  }

  /**
   * Accessor for the contained map. Used for storing the map into the database. It is not intended
   * for client use.
   * @return contained map
   */
  public Map<String, Object> getMap() {
    return map;
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
