package bio.terra.stairway;

import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.stairway.fixtures.FlightsTestPojo;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Test our own test fixtures {@link FlightMapTestUtils}. */
@Tag("unit")
public class FlightMapTestUtilsTest {
  enum MyEnum {
    FOO,
  }

  @Test
  public void serializeAndDeserialize() {
    FlightMapTestUtils.serializeAndDeserialize("foo");
    FlightMapTestUtils.serializeAndDeserialize(42);
    FlightMapTestUtils.serializeAndDeserialize(new FlightsTestPojo().anint(42).astring("foo"));

    assertThrows(
        ClassCastException.class,
        () -> FlightMapTestUtils.serializeAndDeserialize(UUID.randomUUID()));
    assertThrows(
        ClassCastException.class, () -> FlightMapTestUtils.serializeAndDeserialize(MyEnum.FOO));
  }
}
