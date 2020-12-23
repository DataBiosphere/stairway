package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class FlightMapTest {
  private static final String MAP_KEY = "key";
  private static final UUID MAP_VALUE = UUID.randomUUID();

  private ObjectMapper objectMapper = getObjectMapper();

  @Test
  public void recreateFromSerializedString_uuidClass() throws Exception {
    String key = "key";
    // UUID class
    UUID uuid = UUID.randomUUID();
    String strValue = objectMapper.writeValueAsString(uuid);
    FlightInput flightInput = new FlightInput(key, strValue);
    FlightMap map = new FlightMap(ImmutableList.of(flightInput));
    assertEquals(uuid, map.get(key, UUID.class));

    // Final Class
    FinalClass finalClass = new FinalClass();
    finalClass.setValue(1);
    strValue = objectMapper.writeValueAsString(finalClass);
    flightInput = new FlightInput(key, strValue);
    map = new FlightMap(ImmutableList.of(flightInput));
    assertEquals(finalClass, map.get(key, FinalClass.class));

    // Non-final class
    NonFinalClass nonFinalClass = new NonFinalClass();
    nonFinalClass.setValue(1);
    strValue = objectMapper.writeValueAsString(nonFinalClass);
    flightInput = new FlightInput(key, strValue);
    map = new FlightMap(ImmutableList.of(flightInput));
    assertEquals(nonFinalClass, map.get(key, NonFinalClass.class));
  }

  private static final class FinalClass implements java.io.Serializable {
    private int value;

    FinalClass() {}

    public int getvalue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj.getClass() == FinalClass.class && value == ((FinalClass) obj).getvalue();
    }
  }

  private static class NonFinalClass implements java.io.Serializable {
    private int value;

    NonFinalClass() {}

    public int getvalue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj.getClass() == NonFinalClass.class && value == ((NonFinalClass) obj).getvalue();
    }
  }
}
