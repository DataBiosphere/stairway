package bio.terra.stairway;

import bio.terra.stairway.exception.JsonConversionException;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    loadMap(map);
    verifyMap(map);

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
        "[\"java.util.HashMap\",{\"myenum\":[\"bio.terra.stairway.FlightMapTest$MyEnum\",\"FOO\"],\"myuuid\":[\"java.util.UUID\",\"2e74e380-cccd-48ad-a0ac-e006b3650dbe\"],\"mystring\":\"StringValue\",\"mykey\":3,\"mypojo\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

    logger.debug(" In JSON: {}", jsonMap);

    // Verify that a map created from this JSON contains the expected data.
    FlightMap map = new FlightMap();
    map.fromJson(jsonMap);
    verifyMap(map);
  }

  @Test
  public void createTest() {
    FlightMap sourceMap = new FlightMap();
    loadMap(sourceMap);

    List<FlightInput> list = sourceMap.makeFlightInputList();
    String json = sourceMap.toJson();

    // Ignore list, use JSON
    Optional<FlightMap> fromListMap = FlightMap.create(list, json);
    Assertions.assertTrue(fromListMap.isPresent());
    verifyMap(fromListMap.get());

    // Null JSON returns empty map
    Optional<FlightMap> nullJsonEmptyListMap = FlightMap.create(new ArrayList<>(), null);
    Assertions.assertFalse(nullJsonEmptyListMap.isPresent());

    // Bad key in list logs error, but still has good content
    List<FlightInput> badKeyList = new ArrayList<>();
    FlightInput badKeyInput = new FlightInput("badkey", "badval");
    badKeyList.add(badKeyInput);
    Optional<FlightMap> badListKeyMap = FlightMap.create(badKeyList, json);
    Assertions.assertTrue(badListKeyMap.isPresent());
    verifyMap(badListKeyMap.get());
    Assertions.assertThrows(
        RuntimeException.class, () -> badListKeyMap.get().validateAgainst(badKeyList));

    // Bad value in list logs error, but still has good content
    List<FlightInput> badValueList = new ArrayList<>();
    FlightInput badValueInput = new FlightInput(pojoKey, "badval");
    badValueList.add(badValueInput);
    Optional<FlightMap> badListValueMap = FlightMap.create(badValueList, json);
    Assertions.assertTrue(badListValueMap.isPresent());
    verifyMap(badListValueMap.get());
    Assertions.assertThrows(
        JsonProcessingException.class, () -> badListValueMap.get().validateAgainst(badValueList));
  }

  // Intentionally non-static internal class, so that attempting to serialize an instance fails.
  private class NonStaticClass {}

  @Test
  public void serDesExceptions() {
    FlightMap map = new FlightMap();

    // Serializing an unserializable class results in JsonConversionException.
    final String badKey = "bad";
    NonStaticClass unserializable = new NonStaticClass();
    map.put(badKey, unserializable);
    Assertions.assertThrows(JsonConversionException.class, () -> map.toJson());

    // Deserializing the wrong type results in ClassCastException.
    Assertions.assertThrows(ClassCastException.class, () -> map.get(badKey, FlightsTestPojo.class));

    // Deserializing map from bad JSON throws
    Assertions.assertThrows(JsonConversionException.class, () -> map.fromJson("garbage"));
  }
}
