package bio.terra.stairway;

import static bio.terra.stairway.StairwayMapper.getObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class FlightMapTest {
  private static final String MAP_KEY = "key";
  private static final UUID MAP_VALUE = UUID.randomUUID();

  private ObjectMapper objectMapper = getObjectMapper();

  @Test
  public void recreateFromSerializedStringError_simpleEnum() throws Exception {
    SimpleEnum simpleEnum = SimpleEnum.ONE;
    // Server running create flight map, put value inside.
    FlightMap flightMap = new FlightMap();
    flightMap.put(MAP_KEY, simpleEnum);

    // Server restarts, Stairway re-construct flightMap from DB(FlightInput), now everything is
    // String format
    List<FlightInput> flightInput = flightMap.makeFlightInputList();
    FlightMap newMapAfterRestart = new FlightMap(flightInput);

    // Jackson does not know to convert Enum class from String to Enum class
    Assertions.assertThrows(
        ClassCastException.class,
        () -> {
          newMapAfterRestart.get(MAP_KEY, SimpleEnum.class);
        });
  }

  @Test
  public void recreateFromSerializedStringError_enumWithValue() throws Exception {
    // Final Class
    EnumWithValue enumValue = EnumWithValue.ONE;
    String strValue = objectMapper.writeValueAsString(enumValue);
    FlightInput flightInput = new FlightInput(MAP_KEY, strValue);
    FlightMap map = new FlightMap(ImmutableList.of(flightInput));

    Assertions.assertThrows(
        ClassCastException.class,
        () -> {
          map.get(MAP_KEY, EnumWithValue.class);
        });
  }

  @Test
  public void recreateFromSerializedStringError_finalClass() throws Exception {
    FinalClass finalClass = new FinalClass();
    finalClass.setValue(1);
    FlightMap flightMap = new FlightMap();
    flightMap.put(MAP_KEY, finalClass);

    Assertions.assertThrows(
        JsonConversionException.class,
        () -> {
          new FlightMap(flightMap.makeFlightInputList());
        });
  }

  @Test
  public void recreateFromSerializedStringOk_nonFinalClass() throws Exception {
    NonFinalClass nonFinalClass = new NonFinalClass();
    nonFinalClass.setValue(1);
    String strValue = objectMapper.writeValueAsString(nonFinalClass);
    FlightInput flightInput = new FlightInput(MAP_KEY, strValue);
    FlightMap map = new FlightMap(ImmutableList.of(flightInput));
    assertEquals(nonFinalClass, map.get(MAP_KEY, NonFinalClass.class));
  }

  private enum SimpleEnum {
    ONE,
    TWO
  }

  private enum EnumWithValue {
    ONE("ONE"),
    TWO("TWO");

    private final String value;

    EnumWithValue(String value) {
      this.value = value;
    }
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
