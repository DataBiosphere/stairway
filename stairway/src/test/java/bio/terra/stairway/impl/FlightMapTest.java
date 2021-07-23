package bio.terra.stairway.impl;

import bio.terra.stairway.FlightInput;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.JsonConversionException;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class FlightMapTest {

  private static final Logger logger = LoggerFactory.getLogger("FlightMapTest");

  private static final String pojoKey = "mypojo";
  private static final FlightsTestPojo pojoIn = new FlightsTestPojo().anint(99).astring("mystring");

  private static final String intKey = "mykey";
  private static final Integer intIn = 3;

  private static final String stringKey = "mystring";
  private static final String stringIn = "StringValue";

  private static final String uuidKey = "myuuid";
  private static final UUID uuidIn = UUID.fromString("2e74e380-cccd-48ad-a0ac-e006b3650dbe");

  private enum MyEnum {
    FOO,
  }

  private static final String enumKey = "myenum";
  private static final MyEnum enumIn = MyEnum.FOO;

  // JSON created by calling now-removed toJson() method on a FlightMap instance populated by
  // loadMap() using an older version of code.
  private static final String jsonMap =
      "[\"java.util.HashMap\",{\"myenum\":[\"bio.terra.stairway.impl.FlightMapTest$MyEnum\",\"FOO\"],\"myuuid\":[\"java.util.UUID\",\"2e74e380-cccd-48ad-a0ac-e006b3650dbe\"],\"mystring\":\"StringValue\",\"mykey\":3,\"mypojo\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

  private void loadMap(FlightMap outMap) {
    outMap.put(pojoKey, pojoIn);
    outMap.put(intKey, intIn);
    outMap.put(stringKey, stringIn);
    outMap.put(uuidKey, uuidIn);
    outMap.put(enumKey, enumIn);
  }

  private void verifyMap(FlightMap inMap) {
    FlightsTestPojo pojoOut = inMap.get(pojoKey, FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    Integer intOut = inMap.get(intKey, Integer.class);
    Assertions.assertEquals(intIn, intOut);

    String stringOut = inMap.get(stringKey, String.class);
    Assertions.assertEquals(stringIn, stringOut);

    UUID uuidOut = inMap.get(uuidKey, UUID.class);
    Assertions.assertEquals(uuidIn, uuidOut);

    MyEnum enumOut = inMap.get(enumKey, MyEnum.class);
    Assertions.assertEquals(enumOut, enumIn);
  }

  @Test
  public void basic() {

    // Basic test of put/get
    FlightMap map = new FlightMap();
    Assertions.assertTrue(map.isEmpty());
    loadMap(map);
    Assertions.assertFalse(map.isEmpty());
    verifyMap(map);

    FlightMap fromRawMap = new FlightMap();
    fromRawMap.putRaw(pojoKey, map.getRaw(pojoKey));
    fromRawMap.putRaw(intKey, map.getRaw(intKey));
    fromRawMap.putRaw(stringKey, map.getRaw(stringKey));
    fromRawMap.putRaw(uuidKey, map.getRaw(uuidKey));
    fromRawMap.putRaw(enumKey, map.getRaw(enumKey));
    verifyMap(fromRawMap);

    // Test that a non-existent key returns null
    Assertions.assertNull(map.get("key", Object.class));

    // Just making sure this doesn't completely malfunction...
    String output = map.toString();
    Assertions.assertFalse(output.isEmpty());
    logger.debug(output);
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
  public void createTest() {
    FlightMap sourceMap = new FlightMap();
    loadMap(sourceMap);

    List<FlightInput> list = FlightMapUtils.makeFlightInputList(sourceMap);

    // Empty list with null JSON, returns empty map
    FlightMap emptyNullJsonMap = FlightMapUtils.create(new ArrayList<>(), null);
    Assertions.assertTrue(emptyNullJsonMap.isEmpty());

    // Empty list with valid JSON, use JSON
    FlightMap useJsonMap = FlightMapUtils.create(new ArrayList<>(), jsonMap);
    verifyMap(useJsonMap);

    // Otherwise use list
    FlightMap useListMap = FlightMapUtils.create(list, jsonMap);
    verifyMap(useListMap);
  }

  @SuppressFBWarnings(
      value = "SIC_INNER_SHOULD_BE_STATIC",
      justification =
          "Intentionally non-static internal class, so that attempting to serialize an instance fails.")
  private class NonStaticClass {}

  @Test
  public void serDesExceptions() {
    FlightMap map = new FlightMap();
    loadMap(map);

    // Serializing an unserializable class results in JsonConversionException.
    final String badKey = "bad";
    NonStaticClass unserializable = new NonStaticClass();
    Assertions.assertThrows(JsonConversionException.class, () -> map.put(badKey, unserializable));
    Assertions.assertNull(map.get(badKey, NonStaticClass.class));

    // Deserializing the wrong type results in JsonConversionException.
    Assertions.assertThrows(
        JsonConversionException.class, () -> map.get(intKey, FlightsTestPojo.class));

    // Deserializing map from bad JSON throws
    Assertions.assertThrows(
        JsonConversionException.class, () -> FlightMapUtils.create(new ArrayList<>(), "garbage"));
  }
}
