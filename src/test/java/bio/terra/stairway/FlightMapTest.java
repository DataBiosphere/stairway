package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.fixtures.FlightsTestNonPojo;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class FlightMapTest {

  private static final Logger logger = LoggerFactory.getLogger("FlightMapTest");

  private static <T> void testObjectContainer(
      T in, FlightParameterSerializer serializer, FlightParameterDeserializer<T> deserializer) {
    FlightMap.ObjectContainer ocSer = new FlightMap.ObjectContainer(in, serializer);
    String data = ocSer.getData();
    logger.info("Serialized Data: '{}'", data);

    FlightMap.ObjectContainer ocDes = new FlightMap.ObjectContainer(data);
    T out = ocDes.getObject(deserializer);
    Assertions.assertEquals(in, out);
  }

  private static <T> void testObjectContainer(T in, Class<T> type) {
    FlightParameterSerializer serializer = new DefaultFlightParameterSerializer(getObjectMapper());
    FlightParameterDeserializer<T> deserializer =
        new DefaultFlightParameterDeserializer<>(type, getObjectMapper());
    testObjectContainer(in, serializer, deserializer);
  }

  @Test
  public void objectContainers() {
    FlightsTestPojo inPojo = new FlightsTestPojo().anint(99).astring("mystring");
    testObjectContainer(inPojo, FlightsTestPojo.class);

    FlightsTestNonPojo inNonPojo = FlightsTestNonPojo.create(1.99f);
    testObjectContainer(
        inNonPojo, FlightsTestNonPojo.serializer(), FlightsTestNonPojo.deserializer());

    Integer inInt = 100;
    testObjectContainer(inInt, Integer.class);
  }

  @Test
  public void toAndFromJson() {
    FlightMap outMap = new FlightMap();

    FlightsTestPojo pojoIn = new FlightsTestPojo().anint(99).astring("mystring");
    outMap.put("mypojo", pojoIn);

    FlightsTestNonPojo nonPojoIn = FlightsTestNonPojo.create(1.25f);
    outMap.put("mynonpojo", nonPojoIn, FlightsTestNonPojo.serializer());

    Integer intIn = 3;
    outMap.put("myint", intIn);

    String json = outMap.toJson();
    logger.info("JSON: '{}'", json);

    FlightMap inMap = new FlightMap();
    inMap.fromJson(json, FlightDao.FLIGHT_PARAMETERS_VERSION);

    FlightsTestPojo pojoOut = inMap.get("mypojo", FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    FlightsTestNonPojo nonPojoOut = inMap.get("mynonpojo", FlightsTestNonPojo.deserializer());
    Assertions.assertEquals(nonPojoIn, nonPojoOut);

    Integer intOut = inMap.get("myint", Integer.class);
    Assertions.assertEquals(intIn, intOut);
  }

  @Test
  public void demoUpgrade() {
    String json =
        "[\"java.util.HashMap\",{\"mykey\":\"{\\\"uuid\\\":\\\"091fa25d-04ee-447e-b18b-96c1989478dd\\\",\\\"value_numerator\\\":5,\\\"value_denominator\\\":4}\"}]";
    logger.info("Pre-Upgrade JSON: '{}'", json);

    FlightMap inMap = new FlightMap();
    inMap.fromJson(json, FlightDao.FLIGHT_PARAMETERS_VERSION);
    FlightsTestNonPojo nonPojoOut = inMap.get("mykey", FlightsTestNonPojo.deserializer());
    Assertions.assertEquals(nonPojoOut.getValue(), 5.0f / 4.0f);
    Assertions.assertEquals(
        nonPojoOut.getUuid(), UUID.fromString("091fa25d-04ee-447e-b18b-96c1989478dd"));

    inMap.put("mykey", nonPojoOut, FlightsTestNonPojo.serializer());
    logger.info("Post-Upgrade JSON: '{}'", inMap.toJson());
  }

  @Test
  public void fromJsonV1() {
    FlightMap map = new FlightMap();

    String v1json =
        "[\"java.util.HashMap\",{\"mykey\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

    logger.info("In JSON: {}", v1json);
    map.fromJson(v1json, 1);

    String outJson = map.toJson();
    logger.info("Out JSON: {}", outJson);

    FlightsTestPojo pojo = map.get("mykey", FlightsTestPojo.class);
    Assertions.assertNotNull(pojo);
    Assertions.assertEquals("mystring", pojo.getAstring());
    Assertions.assertEquals(99, pojo.getAnint());
  }
}
