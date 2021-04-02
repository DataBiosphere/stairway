package bio.terra.stairway;

/** Utilities for testing with {@link bio.terra.stairway.FlightMap}. */
public class FlightMapTestUtils {
  private FlightMapTestUtils() {}

  /**
   * Tries to serialize and deserialize the object as JSON and a {@link FlightInput} with {@link
   * FlightMap#put(String, Object)} and {@link FlightMap#get(String, Class)}.
   *
   * <p>This is useful for unit testing that arbitrary classes are supported with {@link FlightMap}
   * serialization.
   */
  public static void serializeAndDeserialize(Object object) {
    String key = "objectKey";
    FlightMap flightMap = new FlightMap();
    flightMap.put(key, object);

    FlightMap fromJson = new FlightMap();
    fromJson.fromJson(flightMap.toJson());
    fromJson.get(key, object.getClass());

    FlightMap fromInputs = new FlightMap(flightMap.makeFlightInputList());
    fromInputs.get(key, object.getClass());
  }
}
