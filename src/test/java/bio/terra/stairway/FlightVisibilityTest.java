package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightControlledSleep;
import bio.terra.stairway.flights.TestMinimalFlight;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The intermediate states in this test may not be deterministic. All we can be sure of is that at
 * the end we should have no flights in the flight list.
 */
@Tag("unit")
public class FlightVisibilityTest {
  private static final Logger logger = LoggerFactory.getLogger(FlightVisibilityTest.class);

  @Test
  public void successTest() throws Exception {
    final String stairwayName = "flightVisibilityTest";
    final Duration visibilityInterval = Duration.ofSeconds(15);

    // Start with a clean and shiny database environment.
    Stairway stairway =
        Stairway.newBuilder()
            .stairwayName(stairwayName)
            .completedFlightAvailable(visibilityInterval)
            .enableWorkQueue(false)
            .build();

    List<String> recordedStairways = stairway.initialize(TestUtil.makeDataSource(), true, true);
    stairway.recoverAndStart(recordedStairways);

    String flight1 = launchMinimal(stairway);
    String flight2 = launchMinimal(stairway);
    String flight3 = launchSleeper(stairway);

    // All flights should be visible
    assertTrue(checkFlightState(stairway, flight1));
    assertTrue(checkFlightState(stairway, flight2));
    assertTrue(checkFlightState(stairway, flight3));

    int count = countFlights(stairway);
    assertThat(count, is(equalTo(3)));

    // Wait for visibility interval and a bit more
    TimeUnit.SECONDS.sleep(visibilityInterval.toSeconds() + 2);

    // All completed flights should be invisible, but the active flight should still show
    assertFalse(checkFlightState(stairway, flight1));
    assertFalse(checkFlightState(stairway, flight2));
    assertTrue(checkFlightState(stairway, flight3));
    count = countFlights(stairway);
    assertThat(count, is(equalTo(1)));

    // Release the sleeping flight
    TestPauseController.setControl(1);

    // Wait for visibility interval and a bit more
    TimeUnit.SECONDS.sleep(visibilityInterval.toSeconds() + 2);

    // Now everything should be invisible
    assertFalse(checkFlightState(stairway, flight1));
    assertFalse(checkFlightState(stairway, flight2));
    assertFalse(checkFlightState(stairway, flight3));
    count = countFlights(stairway);
    assertThat(count, is(equalTo(0)));

    // Use the Control object to see all of the flights
    Control control = stairway.getControl();

    // All flights should be visible
    assertTrue(controlFlightState(control, flight1));
    assertTrue(controlFlightState(control, flight2));
    assertTrue(controlFlightState(control, flight3));

    count = controlCountFlights(control);
    assertThat(count, is(equalTo(3)));
  }

  private boolean checkFlightState(Stairway stairway, String flightId) throws Exception {
    try {
      stairway.getFlightState(flightId);
      return true;
    } catch (FlightNotFoundException e) {
      return false;
    }
  }

  private int controlCountFlights(Control control) throws Exception {
    List<FlightState> flights = control.getFlights(0, 100, null);
    return flights.size();
  }

  private boolean controlFlightState(Control control, String flightId) throws Exception {
    try {
      control.getFlightState(flightId);
      return true;
    } catch (FlightNotFoundException e) {
      return false;
    }
  }

  private int countFlights(Stairway stairway) throws Exception {
    List<FlightState> flights = stairway.getFlights(0, 100, null);
    return flights.size();
  }

  private String launchMinimal(Stairway stairway) throws Exception {
    String flightId = UUID.randomUUID().toString();
    stairway.submit(flightId, TestMinimalFlight.class, new FlightMap());
    logger.info("Launched minimal {}", flightId);
    return flightId;
  }

  private String launchSleeper(Stairway stairway) throws Exception {
    String flightId = UUID.randomUUID().toString();
    FlightMap inputs = new FlightMap();
    Integer controlValue = 1;
    inputs.put(MapKey.CONTROLLER_VALUE, controlValue);
    TestPauseController.setControl(0);
    stairway.submit(flightId, TestFlightControlledSleep.class, inputs);
    logger.info("Launched sleeper {}", flightId);
    return flightId;
  }
}
