package bio.terra.stairway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.flights.TestFlightError;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class DismalErrorTest {
  private final Logger logger = LoggerFactory.getLogger(DismalErrorTest.class);

  @Test
  public void dismalErrorTest() throws Exception {
    Stairway stairway = new TestStairwayBuilder().build();

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    String flightId = stairway.createFlightId();
    stairway.submit(flightId, TestFlightError.class, inputParameters);
    logger.debug("Submitted flight id: " + flightId);

    // Wait for done
    FlightState result = stairway.waitForFlight(flightId, null, null);

    // Make sure we get the right error return
    assertEquals(FlightStatus.FATAL, result.getFlightStatus());
    assertTrue(result.getException().isPresent());
    assertEquals("Initial error", result.getException().get().getMessage());
  }
}
