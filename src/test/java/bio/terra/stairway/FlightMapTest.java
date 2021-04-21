package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class FlightMapTest {

  private static final Logger logger = LoggerFactory.getLogger("FlightMapTest");

  private static String pojoKey = "mypojo";
  private static final FlightsTestPojo pojoIn = new FlightsTestPojo().anint(99).astring("mystring");

  private static String intKey = "mykey";
  private static final Integer intIn = 3;

  private static String stringKey = "mystring";
  private static final String stringIn = "StringValue";

  private void loadMap(FlightMap outMap) {
    outMap.put(pojoKey, pojoIn);
    Assertions.assertEquals(pojoIn, outMap.get(pojoKey, FlightsTestPojo.class));

    outMap.put(intKey, intIn);
    Assertions.assertEquals(intIn, outMap.get(intKey, Integer.class));

    outMap.put(stringKey, stringIn);
    Assertions.assertEquals(stringIn, outMap.get(stringKey, String.class));
  }

  private void verifyMap(FlightMap inMap) {
    FlightsTestPojo pojoOut = inMap.get(pojoKey, FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    Integer intOut = inMap.get(intKey, Integer.class);
    Assertions.assertEquals(intIn, intOut);

    String stringOut = inMap.get(stringKey, String.class);
    Assertions.assertEquals(stringIn, stringOut);
  }

  @Test
  public void basic() {

    // Basic test of put/get
    FlightMap map = new FlightMap();
    loadMap(map);
    verifyMap(map);

    // Just making sure this doesn't completely malfunction...
    String output = map.toString();
    Assertions.assertFalse(output.isEmpty());
    logger.debug(output);
  }

  @Test
  public void rawOps() {
    FlightMap map = new FlightMap();
    loadMap(map);

    // Peek each field and poke it back as raw data.
    map.putRaw(pojoKey, map.getRaw(pojoKey));
    map.putRaw(intKey, map.getRaw(intKey));
    map.putRaw(stringKey, map.getRaw(stringKey));

    // Make sure it all still matches.
    verifyMap(map);
  }

  @Test
  public void makeImmutable() {

    // Fill up a map
    FlightMap map = new FlightMap();
    loadMap(map);

    // Make immutable and make sure we can't change it further.
    map.makeImmutable();
    Assertions.assertThrows(UnsupportedOperationException.class, () -> map.put(intKey, intIn + 1));

    // Make sure everything is still intact
    verifyMap(map);
  }

  @Test
  public void toAndFromJson() {
    FlightMap outMap = new FlightMap();

    // Test that a non-existent key returns null
    Assertions.assertNull(outMap.get("key", Object.class));

    loadMap(outMap);

    String json = outMap.toJson();
    logger.debug("JSON: '{}'", json);

    FlightMap inMap = new FlightMap();
    inMap.fromJson(json);
    verifyMap(inMap);
  }

  @Test
  public void fromJsonMap() {

    // JSON created by calling toJson() on a map instance populated by loadMap() using an older
    // version of code.
    String jsonMap =
        "[\"java.util.HashMap\",{\"mystring\":\"StringValue\",\"mykey\":3,\"mypojo\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

    logger.debug(" In JSON: {}", jsonMap);

    // Verify that a map created from this JSON contains the expected data.
    FlightMap map = new FlightMap();
    map.fromJson(jsonMap);
    verifyMap(map);

    // Make sure the output JSON matches the input JSON
    String outJson = map.toJson();
    logger.debug("Out JSON: {}", outJson);
    Assertions.assertEquals(jsonMap, outJson);
  }

  @Test
  public void createTest() {
    FlightMap sourceMap = new FlightMap();
    loadMap(sourceMap);

    List<FlightInput> list = sourceMap.makeFlightInputList();
    String json = sourceMap.toJson();

    Optional<FlightMap> fromListMap = FlightMap.create(list, json);
    Assertions.assertTrue(fromListMap.isPresent());
    verifyMap(fromListMap.get());

    Optional<FlightMap> jsonEmptyListMap = FlightMap.create(new ArrayList<>(), json);
    Assertions.assertTrue(jsonEmptyListMap.isPresent());
    verifyMap(jsonEmptyListMap.get());

    Optional<FlightMap> nullJsonEmptyListMap = FlightMap.create(new ArrayList<>(), null);
    Assertions.assertFalse(nullJsonEmptyListMap.isPresent());
  }

  // Intentionally non-static internal class, so that attempting to serialize an instance fails.
  private class NonStaticClass {}

  @Test
  public void serDesExceptions() {
    FlightMap map = new FlightMap();

    // Serializing an unserializable class results in JsonConversionException.
    final String badKey = "bad";
    NonStaticClass unserializable = new NonStaticClass();
    Assertions.assertThrows(JsonConversionException.class, () -> map.put(badKey, unserializable));

    // Deserializing the wrong type results in ClassCastException.
    map.put(badKey, "garbage");
    Assertions.assertThrows(ClassCastException.class, () -> map.get(badKey, FlightsTestPojo.class));

    // Deserializing map from bad JSON throws
    Assertions.assertThrows(JsonConversionException.class, () -> map.fromJson("garbage"));
  }
}
