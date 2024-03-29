package bio.terra.stairway;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlight;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class StairwayClassTest {
  private Stairway stairway;

  @BeforeEach
  public void setup() throws Exception {
    stairway = new TestStairwayBuilder().build();
  }

  @Test
  public void testNullFlightClass() throws Exception {
    FlightMap flightMap = new FlightMap();
    Assertions.assertThrows(
        MakeFlightException.class,
        () -> {
          stairway.submit("nullflightclass", null, flightMap);
        });
  }

  @Test
  public void testNullInputParams() throws Exception {
    Assertions.assertThrows(
        MakeFlightException.class,
        () -> {
          stairway.submit("nullinput", TestFlight.class, null);
        });
  }

  @Test
  public void testBadFlightDone() throws StairwayException {
    Assertions.assertThrows(
        FlightNotFoundException.class,
        () -> {
          TestUtil.isDone(stairway, "abcdefg");
        });
  }

  @Test
  public void testBadFlightGetResult() throws StairwayException {
    Assertions.assertThrows(
        FlightNotFoundException.class,
        () -> {
          stairway.getFlightState("abcdefg");
        });
  }
}
