package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.stairway.fixtures.FileQueue;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class SleepQueueTest {
  private final Logger logger = LoggerFactory.getLogger(SleepQueueTest.class);

  @Test
  public void clusterSuccessTest() throws Exception {
    QueueInterface workQueue = FileQueue.makeFileQueue("clusterSuccessTest");

    Stairway stairway1 = new TestStairwayBuilder().name("stairway1").workQueue(workQueue).build();

    Stairway stairway2 =
        new TestStairwayBuilder().name("stairway2").workQueue(workQueue).continuing(true).build();

    SleepQueueThread sqt1 = new SleepQueueThread(stairway1, true);
    SleepQueueThread sqt2 = new SleepQueueThread(stairway2, false);

    Thread thread1 = new Thread(sqt1);
    Thread thread2 = new Thread(sqt2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    TimeUnit.SECONDS.sleep(120);
    stairway1.terminate(5, TimeUnit.SECONDS);
    stairway2.quietDown(5, TimeUnit.SECONDS);
    stairway2.terminate(5, TimeUnit.SECONDS);

    List<String> flightList = sqt1.getFlightIdList();
    flightList.addAll(sqt2.getFlightIdList());
    for (String flightId : flightList) {
      FlightState flightState = stairway1.getFlightState(flightId);
      logger.info("Flight " + flightId + " status: " + flightState.getFlightStatus());
    }

    for (String flightId : flightList) {
      FlightState flightState = stairway2.getFlightState(flightId);
      assertThat(
          "flightStatus is success", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
    }
  }
}
