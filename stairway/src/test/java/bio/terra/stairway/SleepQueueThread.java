package bio.terra.stairway;

import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.flights.TestFlightSleep;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepQueueThread implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(SleepQueueThread.class);
  private static final Integer SLEEP_SECONDS = 5;
  private static final Integer FLIGHTS_TO_LAUNCH = 10;

  private final boolean shutdownAfter;
  private final Stairway stairway;
  private final List<String> flightIdList;

  public SleepQueueThread(Stairway stairway, boolean shutdownAfter) {
    this.shutdownAfter = shutdownAfter;
    this.stairway = stairway;
    this.flightIdList = new ArrayList<>();
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < FLIGHTS_TO_LAUNCH; i++) {
        makeSleepFlight(stairway, stairway.getStairwayName() + "-flight-" + i);
      }
      if (shutdownAfter) {
        TimeUnit.SECONDS.sleep(SLEEP_SECONDS);
        logger.info("Stairway " + stairway.getStairwayName() + " quieting down");
        stairway.quietDown(5, TimeUnit.SECONDS);
      }
    } catch (Exception ex) {
      logger.error("SleepQueueThread caught exception", ex);
    }
  }

  public List<String> getFlightIdList() {
    return flightIdList;
  }

  private void makeSleepFlight(Stairway stairway, String result) throws Exception {
    Integer sleepSeconds = 5;
    FlightMap inputParams = new FlightMap();
    inputParams.put(MapKey.SLEEP_SECONDS, sleepSeconds);
    inputParams.put(MapKey.RESULT, result);
    String flightId = stairway.createFlightId();
    flightIdList.add(flightId);
    logger.info("submitToQueue stairway: " + stairway.getStairwayName() + " flightId: " + flightId);
    stairway.submitToQueue(flightId, TestFlightSleep.class, inputParams);
  }
}
