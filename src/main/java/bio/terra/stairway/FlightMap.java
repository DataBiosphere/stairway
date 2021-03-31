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
import org.springframework.lang.Nullable;

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
    this();
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

  /** Convert the map to an unmodifiable form. */
  public void makeImmutable() {
    map = Collections.unmodifiableMap(map);
  }

  /**
   * Return the object from the hash map cast to the right type using default Jackson object
   * deserialization for the passed object type. Throw an exception if the Object cannot be cast to
   * that type.
   *
   * @param <T> - type of class to expect in the hash map
   * @param key - key to lookup in the hash map
   * @param type - class requested
   * @return null if not found
   * @throws ClassCastException if found, not castable to the requested type
   */
  @Nullable
  public <T> T get(String key, Class<T> type) {
    FlightParameterDeserializer<T> deserializer =
        new DefaultFlightParameterDeserializer<>(type, getObjectMapper());
    return get(key, deserializer);
  }

  /**
   * Return the object from the hash map cast to the right type using a custom deserializer. Throw
   * an exception if the Object cannot be cast to that type.
   *
   * @param <T> - type of class to expect in the hash map
   * @param key - key to lookup in the hash map
   * @param deserializer - custom deserializer implementing interface {@code
   *     FlightParameterDeserializer<T>}
   * @return null if not found
   * @throws ClassCastException if found, not castable to the requested type
   */
  @Nullable
  public <T> T get(String key, FlightParameterDeserializer<T> deserializer) {
    String serializedObject = map.get(key);
    return (serializedObject != null) ? deserializer.deserialize(serializedObject) : null;
  }

  /**
   * Place an object into the hash map under a given key using default Jackson object serialization
   * for the object's type.
   *
   * @param key - key to store object under in the hash map
   * @param value - object to be stored
   */
  public <T> void put(String key, T value) {
    FlightParameterSerializer<T> serializer =
        new DefaultFlightParameterSerializer<T>(getObjectMapper());
    put(key, value, serializer);
  }

  /**
   * Place an object into the hash map under a given key using a custom serializer
   *
   * @param key - key to store object under in the hash map
   * @param value - object to be stored
   * @param serializer - custom serializer implementing interface FlightParameterSerializer
   */
  public <T> void put(String key, T value, FlightParameterSerializer<T> serializer) {
    map.put(key, serializer.serialize(value));
  }

  public String toJson() {
    try {
      return getObjectMapper().writeValueAsString(map);
    } catch (JsonProcessingException ex) {
      throw new JsonConversionException("Failed to convert map to json string", ex);
    }
  }

  private void fromJsonV1(String json) {
    try {
      Map<String, JsonNode> legacyMap =
          getObjectMapper().readValue(json, new TypeReference<Map<String, JsonNode>>() {});
      for (Map.Entry<String, JsonNode> entry : legacyMap.entrySet()) {
        map.put(entry.getKey(), entry.getValue().toString());
      }
    } catch (IOException ex) {
      throw new JsonConversionException("Failed to convert json string to map", ex);
    }
  }

  public void fromJson(String json, int version) {
    if (version == 1) {
      fromJsonV1(json);
    } else {
      try {
        map = getObjectMapper().readValue(json, new TypeReference<Map<String, String>>() {});
      } catch (IOException ex) {
        throw new JsonConversionException("Failed to convert json string to map", ex);
      }
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
