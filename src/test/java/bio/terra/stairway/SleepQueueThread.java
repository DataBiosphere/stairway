package bio.terra.stairway;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepQueueThread implements Runnable {
  private Logger logger = LoggerFactory.getLogger(SleepQueueThread.class);

  private boolean shutdownAfter;
  private Stairway stairway;
  private Integer sleepSeconds = 5;
  private Integer flightsToLaunch = 10;
  private List<String> flightIdList;

  public SleepQueueThread(Stairway stairway, boolean shutdownAfter) {
    this.shutdownAfter = shutdownAfter;
    this.stairway = stairway;
    this.flightIdList = new ArrayList<>();
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < flightsToLaunch; i++) {
        makeSleepFlight(stairway, stairway.getStairwayName() + "-flight-" + i);
      }
      if (shutdownAfter) {
        TimeUnit.SECONDS.sleep(sleepSeconds);
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
