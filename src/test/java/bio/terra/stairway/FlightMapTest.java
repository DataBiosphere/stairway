package bio.terra.stairway;

import bio.terra.stairway.fixtures.FlightsTestPojo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class FlightMapTest {

  private static final Logger logger = LoggerFactory.getLogger("FlightMapTest");

  @Test
  public void toAndFromJson() {
    FlightMap outMap = new FlightMap();

    FlightsTestPojo pojoIn = new FlightsTestPojo().anint(99).astring("mystring");
    outMap.put("mypojo", pojoIn);

    Integer intIn = 3;
    outMap.put("myint", intIn);

    String json = outMap.toJson();
    logger.info("JSON: '{}'", json);

    FlightMap inMap = new FlightMap();
    inMap.fromJson(json);

    FlightsTestPojo pojoOut = inMap.get("mypojo", FlightsTestPojo.class);
    Assertions.assertEquals(pojoIn, pojoOut);

    Integer intOut = inMap.get("myint", Integer.class);
    Assertions.assertEquals(intIn, intOut);
  }

  @Test
  public void fromJsonMap() {
    FlightMap map = new FlightMap();

    String jsonMap =
        "[\"java.util.HashMap\",{\"mykey\":[\"bio.terra.stairway.fixtures.FlightsTestPojo\",{\"astring\":\"mystring\",\"anint\":99}]}]";

    logger.debug(" In JSON: {}", jsonMap);
    map.fromJson(jsonMap);

    String outJson = map.toJson();
    logger.debug("Out JSON: {}", outJson);
    Assertions.assertEquals(jsonMap, outJson);

    FlightsTestPojo pojo = map.get("mykey", FlightsTestPojo.class);
    Assertions.assertNotNull(pojo);
    Assertions.assertEquals("mystring", pojo.getAstring());
    Assertions.assertEquals(99, pojo.getAnint());
  }
}
