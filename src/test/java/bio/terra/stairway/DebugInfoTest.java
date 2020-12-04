package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlight;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The debug info test needs to ensure that the flight actually pauses between steps so it can be
 * restarted. To do this we use a two step Flight. We start the flight, and then ensure after the
 * initial submission the flight is left in a ready state and can be resumed. NOTE: This only works
 * for the initial submit currently as we have not persisted FlightDebugInfo for reading from the
 * database yet.
 */
@Tag("unit")
public class DebugInfoTest {

  private Logger logger = LoggerFactory.getLogger(DebugInfoTest.class);

  @Test
  public void restartEachStepTrueTest() throws Exception {
    final String stairwayName = "restartEachStepTrueTest";
    String flightId = "restartEachStepTrueTest";

    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightDao flightDao = stairway.getFlightDao();
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().restartEachStep(true).build();

    String filename = makeFilename();
    FlightFilter filter = new FlightFilter();

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    inputParameters.put("filename", filename);
    inputParameters.put("text", "testing 1 2 3");
    stairway.submitWithDebugInfo(flightId, TestFlight.class, inputParameters, true, debugInfo);
    logger.debug("Submitted flight id: " + flightId);

    // Sleep to ensure the first step completes.
    TimeUnit.SECONDS.sleep(5);

    // We expect the flight to not be done, but to be in the database in READY state so it can be
    // resumed.
    List<String> readyFlights = flightDao.getReadyFlights();
    assertThat("One Ready Flight", readyFlights.size(), equalTo(1));
    assertThat(readyFlights.get(0), CoreMatchers.equalTo(flightId));

    // Resume the test flight.
    stairway.resume(flightId);

    // Wait for done
    FlightState result = stairway.waitForFlight(flightId, null, null);
    assertTrue(TestUtil.isDone(stairway, flightId));
    readyFlights = flightDao.getReadyFlights();
    assertThat("No ready flights", readyFlights.size(), equalTo(0));
    assertThat(result.getFlightStatus(), CoreMatchers.equalTo(FlightStatus.SUCCESS));
    assertFalse(result.getException().isPresent());

    // We expect the existent filename to still be there
    File file = new File(filename);
    assertTrue(file.exists());

    try {
      stairway.deleteFlight(flightId, false);
      stairway.waitForFlight(flightId, null, null);
    } catch (FlightNotFoundException ex) {
      assertThat(ex.getMessage(), containsString(flightId));
    }
  }

  private String makeFilename() {
    return "/tmp/test." + UUID.randomUUID().toString() + ".txt";
  }
}
