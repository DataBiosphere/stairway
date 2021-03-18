package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDelegatingDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.common.annotations.VisibleForTesting;
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
  @VisibleForTesting
  static final class ObjectContainer {
    private final String data;
    private final Object object;
    private final FlightParameterSerializer serializer;

    public ObjectContainer(String data) {
      this.data = data;
      this.object = null;
      this.serializer = null;
    }

    public ObjectContainer(Object object, FlightParameterSerializer serializer) {
      this.data = null;
      this.object = object;
      this.serializer = serializer;
    }

    public String getData() {
      // Constructors ensure that one can only have valid data OR object+serializer.
      return (data != null) ? data : serializer.serialize(object);
    }

    public <T> T getObject(FlightParameterDeserializer<T> deserializer) {
      return (object != null) ? deserializer.safeCast(object) : deserializer.deserialize(data);
    }
  }

  private static class ObjectContainerToStringConverter
      extends StdConverter<ObjectContainer, String> {
    @Override
    public String convert(ObjectContainer value) {
      return value.getData();
    }
  }

  private static class StringToObjectContainerConverter
      extends StdConverter<String, ObjectContainer> {
    @Override
    public ObjectContainer convert(String value) {
      return new ObjectContainer(value);
    }
  }

  private Map<String, ObjectContainer> map;

  public FlightMap() {
    map = new HashMap<>();

    SimpleModule module = new SimpleModule();

    module.addSerializer(
        ObjectContainer.class, new StdDelegatingSerializer(new ObjectContainerToStringConverter()));

    module.addDeserializer(
        ObjectContainer.class,
        new StdDelegatingDeserializer<>(new StringToObjectContainerConverter()));

    getObjectMapper().registerModule(module);
  }

  /**
   * Alternate constructor, used by the DAO to re-create FlightMap from its serialized form.
   *
   * @param inputList input list form of the input parameters
   */
  FlightMap(List<FlightInput> inputList) {
    this();
    for (FlightInput input : inputList) {
      map.put(input.getKey(), new ObjectContainer(input.getValue()));
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
    for (Map.Entry<String, ObjectContainer> entry : map.entrySet()) {
      inputList.add(new FlightInput(entry.getKey(), entry.getValue().getData()));
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
  @Deprecated
  @Nullable
  public <T> T get(String key, Class<T> type) {
    FlightParameterDeserializer<T> deserializer =
        new DefaultFlightParameterDeserializer<>(type, getObjectMapper());
    return get(key, deserializer);
  }

  @Nullable
  public <T> T get(String key, FlightParameterDeserializer<T> deserializer) {
    ObjectContainer container = map.get(key);
    return (container != null) ? container.getObject(deserializer) : null;
  }

  public void put(String key, Object value, FlightParameterSerializer serializer) {
    map.put(key, new ObjectContainer(value, serializer));
  }

  @Deprecated
  public void put(String key, Object value) {
    FlightParameterSerializer serializer = new DefaultFlightParameterSerializer(getObjectMapper());
    put(key, value, serializer);
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
        map.put(entry.getKey(), new ObjectContainer(entry.getValue().toString()));
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
        map =
            getObjectMapper().readValue(json, new TypeReference<Map<String, ObjectContainer>>() {});
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
