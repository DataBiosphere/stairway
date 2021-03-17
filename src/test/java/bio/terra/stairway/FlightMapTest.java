package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;

import bio.terra.stairway.fixtures.FlightsTestPojo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class FlightMapTest {

  private static final Logger logger = LoggerFactory.getLogger("FlightMapTest");

  private static <T> void testObjectContainer(T in, Class<T> type) {
    FlightParameterSerializer serializer = new DefaultFlightParameterSerializer(getObjectMapper());
    FlightMap.ObjectContainer ocSer = new FlightMap.ObjectContainer(in, serializer);
    String data = ocSer.getData();
    logger.info("Serialized Data: '{}'", data);

    FlightMap.ObjectContainer ocDes = new FlightMap.ObjectContainer(data);
    FlightParameterDeserializer<T> deserializer =
        new DefaultFlightParameterDeserializer<>(type, getObjectMapper());

    T out = ocDes.getObject(deserializer);
    Assertions.assertEquals(in, out);
  }

  @Test
  public void objectContainers() {
    FlightsTestPojo inPojo = new FlightsTestPojo().anint(99).astring("mystring");
    testObjectContainer(inPojo, FlightsTestPojo.class);

    Integer inInt = 100;
    testObjectContainer(inInt, Integer.class);
  }

  @Test
  public void toAndFromJson() {
    FlightMap outMap = new FlightMap();
    FlightsTestPojo pojoIn = new FlightsTestPojo().anint(99).astring("mystring");
    outMap.put("mykey", pojoIn);

    Integer intIn = 3;
    outMap.put("myint", intIn);

    String json = outMap.toJson();
    logger.info("JSON: '{}'", json);

    FlightMap inMap = new FlightMap();
    inMap.fromJson(json, FlightMap.METADATA_VERSION);

    FlightsTestPojo pojoOut = inMap.get("mykey", FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    Integer intOut = inMap.get("myint", Integer.class);
    Assertions.assertEquals(intIn, intOut);
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
