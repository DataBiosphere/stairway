package bio.terra.stairway;

import bio.terra.stairway.fixtures.FlightsTestPojo;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*

Imagine that we wish to remove the following class, which may exist in
outstanding flights at upgrade time.  (Note that a more likely scenario is that
we move DefunctPojo to a different project; this is merely for illustrative
purposes).

class DefunctPojo {
  private UUID uuid;
  private float afloat;
  private String astring;

  ...
}

Adding this to a FlightMap and serializing the FlightMap results the following
JSON (pretty-printed):

[
    "java.util.HashMap",
    {
        "defunct": [
            "bio.terra.stairway.DefunctPojo",
            {
                "afloat": 1.9,
                "astring": "legacy",
                "uuid": "d2acb963-0968-45b8-8098-288fda3b71db"
            }
        ]
    }
]

When the DefunctPojo class is removed, deserializing this object will fail in
the FlightMap#fromJson() method, as it will not be able to resolve the type
for the DefunctPojo object.  As this deserialization happens in the guts of
Flight deserialization, there is a challenge for Flight implementers to be able
to handle and remediate this exception at deserialization time.

The test below illustrates a possible scheme whereby Flight implementers could
register FlightMapUpgrader class(es) with their FlightMap instances to filter
JSON

*/

@Tag("unit")
public class FlightMapTest {

  private static Logger logger = LoggerFactory.getLogger("FlightMapTest");

  private class MyUpgrader implements FlightMapUpgrader {
    @Override
    public FlightMapUpgradeView upgrade(FlightMapUpgradeView view) {

      String type = view.getType("defunct");
      Assertions.assertEquals("bio.terra.stairway.DefunctPojo", type);

      JsonNode node = view.getJson("defunct");
      String astring = node.get("astring").textValue();
      float afloat = node.get("afloat").floatValue();

      view.replace("defunct", new FlightsTestPojo().anint(Math.round(afloat)).astring(astring));

      return view;
    }
  }

  @Test
  public void upgrade() {
    FlightMap map = new FlightMap(new FlightMapTest.MyUpgrader());
    String json =
        "[\"java.util.HashMap\",{\"defunct\":[\"bio.terra.stairway.DefunctPojo\",{\"uuid\":\"d2acb963-0968-45b8-8098-288fda3b71db\",\"afloat\":1.9,\"astring\":\"legacy\"}]}]";
    map.fromJson(json);
    FlightsTestPojo pojo = map.get("defunct", FlightsTestPojo.class);
    Assertions.assertEquals("legacy", pojo.getAstring());
    Assertions.assertEquals(2, pojo.getAnint());
  }
}
