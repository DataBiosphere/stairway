package bio.terra.stairway;

import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestMinimalFlight;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * The intermediate states in this test may not be deterministic. All we can be sure of is that at
 * the end we should have no flights in the flight list.
 */
@Tag("unit")
public class FlightCleanerTest {
  private static final Logger logger = LoggerFactory.getLogger(FlightCleanerTest.class);

  @Test
  public void successTest() throws Exception {
    final String stairwayName = "flightCleanerTest";

    // Start with a clean and shiny database environment.
    Stairway stairway =
        Stairway.newBuilder()
            .stairwayName(stairwayName)
            .completedFlightRetention(Duration.ofMinutes(1))
            .retentionCheckInterval(Duration.ofSeconds(15))
            .enableWorkQueue(false)
            .build();

    List<String> recordedStairways = stairway.initialize(TestUtil.makeDataSource(), true, true);
    stairway.recoverAndStart(recordedStairways);

    for (int i = 0; i < 5; i++) {
      logger.info("Flight count: {}", countFlights(stairway));
      runFlights(stairway);
      TimeUnit.SECONDS.sleep(15);
    }

    for (int i = 0; i < 5; i++) {
      logger.info("Flight count: {}", countFlights(stairway));
      TimeUnit.SECONDS.sleep(15);
    }

    int count = countFlights(stairway);
    assertThat(count, is(equalTo(0)));
  }

  private void runFlights(Stairway stairway) throws Exception {
    for (int i = 0; i < 3; i++) {
      String flightId = UUID.randomUUID().toString();
      stairway.submit(flightId, TestMinimalFlight.class, new FlightMap());
      logger.info("Launched {}", flightId);
    }
  }

  private int countFlights(Stairway stairway) throws Exception {
    List<FlightState> flights = stairway.getFlights(0, 100, null);
    return flights.size();
  }
}
