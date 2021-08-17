package bio.terra.stairway.impl;

import bio.terra.stairway.FlightInput;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.JsonConversionException;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final String enumKey = "myenum";
  private static final MyEnum enumIn = MyEnum.FOO;
  // JSON created by calling now-removed toJson() method on a FlightMap instance populated by
  // loadMap() using an older version of code.
  private static final String jsonMap =
      "[\"java.util.HashMap\",{\"myenum\":[\"bio.terra.stairway.impl.FlightMapTest$MyEnum\",\"FOO\"],\"myuuid\":[\"java.util.UUID\",\"2e74e380-cccd-48ad-a0ac-e006b3650dbe\"],\"mystring\":\"StringValue\",\"mykey\":3,\"mypojo\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

  private void loadMap(FlightMap outMap) {
    Assertions.assertFalse(outMap.containsKey(pojoKey));
    outMap.put(pojoKey, pojoIn);

    Assertions.assertFalse(outMap.containsKey(intKey));
    outMap.put(intKey, intIn);

    Assertions.assertFalse(outMap.containsKey(stringKey));
    outMap.put(stringKey, stringIn);

    Assertions.assertFalse(outMap.containsKey(uuidKey));
    outMap.put(uuidKey, uuidIn);

    Assertions.assertFalse(outMap.containsKey(enumKey));
    outMap.put(enumKey, enumIn);
  }

  private void verifyMap(FlightMap inMap) {
    Assertions.assertTrue(inMap.containsKey(pojoKey));
    FlightsTestPojo pojoOut = inMap.get(pojoKey, FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    pojoOut = inMap.get(pojoKey, new TypeReference<>() {});
    Assertions.assertEquals(pojoIn, pojoOut);

    Assertions.assertTrue(inMap.containsKey(intKey));
    Integer intOut = inMap.get(intKey, Integer.class);
    Assertions.assertEquals(intIn, intOut);

    intOut = inMap.get(intKey, new TypeReference<>() {});
    Assertions.assertEquals(intIn, intOut);

    Assertions.assertTrue(inMap.containsKey(stringKey));
    String stringOut = inMap.get(stringKey, String.class);
    Assertions.assertEquals(stringIn, stringOut);

    stringOut = inMap.get(stringKey, new TypeReference<>() {});
    Assertions.assertEquals(stringIn, stringOut);

    Assertions.assertTrue(inMap.containsKey(uuidKey));
    UUID uuidOut = inMap.get(uuidKey, UUID.class);
    Assertions.assertEquals(uuidIn, uuidOut);

    uuidOut = inMap.get(uuidKey, new TypeReference<>() {});
    Assertions.assertEquals(uuidIn, uuidOut);

    Assertions.assertTrue(inMap.containsKey(enumKey));
    MyEnum enumOut = inMap.get(enumKey, MyEnum.class);
    Assertions.assertEquals(enumOut, enumIn);

    enumOut = inMap.get(enumKey, new TypeReference<>() {});
    Assertions.assertEquals(enumIn, enumOut);
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

  @Test
  public void guavaTypes() {
    FlightMap flightMap = new FlightMap();

    final String key = "immutableList";
    ImmutableList<Integer> immutableListIn = ImmutableList.of(1, 2, 3, 4);
    flightMap.put(key, immutableListIn);

    ImmutableList<Integer> immutableListOut = flightMap.get(key, ImmutableList.class);
    Assertions.assertEquals(immutableListIn, immutableListOut);
  }

  @Test
  public void typeRef() {
    FlightMap flightMap = new FlightMap();
    final String key = "key";

    Map<MyEnum, FlightsTestPojo> testMapIn = new HashMap<>();
    testMapIn.put(MyEnum.FOO, new FlightsTestPojo().anint(1).astring("first"));
    testMapIn.put(MyEnum.BAR, new FlightsTestPojo().anint(2).astring("second"));
    testMapIn.put(MyEnum.BAZ, new FlightsTestPojo().anint(3).astring("third"));
    flightMap.put(key, testMapIn);

    // This overload does not support parameterized types; any type that is not castable from String
    // will cause failures upon use of the Map.
    Map<MyEnum, FlightsTestPojo> testMapOut = flightMap.get(key, Map.class);
    Assertions.assertNotEquals(testMapIn, testMapOut);

    // This overload supports parameterized types and will correctly deserialize the parameter
    // types.
    testMapOut = flightMap.get(key, new TypeReference<>() {});
    Assertions.assertEquals(testMapIn, testMapOut);

    // This will silently allow us to deserialize the wrong type, but using the map will
    // subsequently fail.
    Map<UUID, FlightsTestPojo> badMapOut = flightMap.get(key, Map.class);
    for (Map.Entry<UUID, FlightsTestPojo> badEntry : badMapOut.entrySet()) {
      Assertions.assertThrows(
          ClassCastException.class,
          () -> {
            String foo = badEntry.getKey().toString();
            System.out.println("Nothing should get printed: " + foo);
          });
    }

    // This version will throw an exception if deserialization of the requested type is not
    // possible.
    Assertions.assertThrows(
        JsonConversionException.class,
        () -> flightMap.get(key, new TypeReference<Map<UUID, FlightsTestPojo>>() {}));
  }

  private enum MyEnum {
    FOO,
    BAR,
    BAZ,
  }

  @SuppressFBWarnings(
      value = "SIC_INNER_SHOULD_BE_STATIC",
      justification =
          "Intentionally non-static internal class, so that attempting to serialize an instance fails.")
  private class NonStaticClass {}
}
