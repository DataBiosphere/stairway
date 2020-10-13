package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightControlledSleep;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("connected")
public class QueueFlightTest {
  private Logger logger = LoggerFactory.getLogger(QueueFlightTest.class);

  @Test
  public void queueFlightTest() throws Exception {
    // Submit directly to queue and make sure we end up in the right state
    Stairway stairway = TestUtil.setupConnectedStairway("queueFlightTest", false);

    FlightMap inputs = new FlightMap();
    int controlValue = 1;
    inputs.put(MapKey.CONTROLLER_VALUE, controlValue);

    TestPauseController.setControl(0);
    String queuedFlightId = "queuedTest";
    stairway.submitToQueue(queuedFlightId, TestFlightControlledSleep.class, inputs);

    FlightState flightState = stairway.getFlightState(queuedFlightId);
    assertThat("flight got queued", flightState.getFlightStatus(), equalTo(FlightStatus.QUEUED));

    // Free the paused flights
    TestPauseController.setControl(controlValue);
    stairway.waitForFlight(queuedFlightId, null, null);

    flightState = stairway.getFlightState(queuedFlightId);
    assertThat("flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
  }

  @Test
  public void admissionControlTest() throws Exception {
    // We create a one thread work queue with 0 tolerance for queued flights.
    // Put one flight in that pauses.
    // Put another flight in. It should arrive in the QUEUED state
    String projectId = TestUtil.getEnvVar("GOOGLE_CLOUD_PROJECT", null);
    assertNotNull(projectId);

    DataSource dataSource = TestUtil.makeDataSource();
    Stairway stairway =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName("admissionControlTest")
            .enableWorkQueue(true)
            .workQueueProjectId(projectId)
            .maxParallelFlights(1)
            .maxQueuedFlights(1)
            .build();
    List<String> recordedStairways = stairway.initialize(dataSource, true, true);
    assertThat("no stairway to recover", recordedStairways.size(), equalTo(0));
    stairway.recoverAndStart(null);

    FlightMap inputs = new FlightMap();
    Integer controlValue = 1;
    inputs.put(MapKey.CONTROLLER_VALUE, controlValue);

    // The first flight should run and get a thread
    TestPauseController.setControl(0);
    String runningFlightId = "runningFlight";
    stairway.submit(runningFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    // The second flight should be RUNNING, but be queued in the thread pool
    String threadQueuedFlightId = "threadQueuedFlight";
    stairway.submit(threadQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    FlightState flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight queued to thread pool ",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The third flight queued to the work queue, but run immediately, since there is an
    // outstanding pull request.
    String workQueuedRunFlightId = "workRunQueuedFlight";
    stairway.submit(workQueuedRunFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight run from work queue",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The fourth flight is queued to the work queue, and not run, since the queue depth
    // stops the listener from trying to pull more from the work queue.
    String workQueuedFlightId = "workQueuedFlight";
    stairway.submit(workQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    flightState = stairway.getFlightState(workQueuedFlightId);
    assertThat(
        "4th flight stays in work queue",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.QUEUED));

    // Free the paused flights
    TestPauseController.setControl(controlValue);
    stairway.waitForFlight(workQueuedFlightId, null, null);

    flightState = stairway.getFlightState(workQueuedFlightId);
    assertThat(
        "4th flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(runningFlightId);
    assertThat(
        "1st flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
  }

  @Test
  public void supplyQueueTest() throws Exception {
    String projectId = TestUtil.getEnvVar("GOOGLE_CLOUD_PROJECT", null);
    DataSource dataSource = TestUtil.makeDataSource();
    String topicId = "supplyQueue-topic";
    String subscriptionId = "supplyQueue-sub";
    QueueCreate.makeTopic(projectId, topicId);
    QueueCreate.makeSubscription(projectId, topicId, subscriptionId);

    Stairway stairway =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName("admissionControlTest")
            .enableWorkQueue(true)
            .workQueueProjectId(projectId)
            .workQueueTopicId(topicId)
            .workQueueSubscriptionId(subscriptionId)
            .maxParallelFlights(1)
            .maxQueuedFlights(1)
            .build();
    List<String> recordedStairways = stairway.initialize(dataSource, true, true);
    assertThat("no stairway to recover", recordedStairways.size(), equalTo(0));
    stairway.recoverAndStart(null);

    FlightMap inputs = new FlightMap();
    Integer controlValue = 1;
    inputs.put(MapKey.CONTROLLER_VALUE, controlValue);

    // The first flight should get pulled from the queue and run, and will wait
    TestPauseController.setControl(0);
    String runningFlightId = "runningFlight";
    stairway.submitToQueue(runningFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    // The second flight should get pulled from the queue and be queued in the thread pool.
    String threadQueuedFlightId = "threadQueuedFlight";
    stairway.submitToQueue(threadQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(2);

    FlightState flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight queued to thread pool ",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The third flight queued to the work queue and not be run
    String workQueuedRunFlightId = "workQueuedFlight1";
    stairway.submitToQueue(workQueuedRunFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight In work queue", flightState.getFlightStatus(), equalTo(FlightStatus.QUEUED));

    // The fourth flight is queued to the work queue, and not run
    String workQueuedFlightId = "workQueuedFlight2";
    stairway.submitToQueue(workQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(1);

    flightState = stairway.getFlightState(workQueuedFlightId);
    assertThat(
        "4th flight stays in work queue",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.QUEUED));

    // Free the paused flights
    TestPauseController.setControl(controlValue);
    stairway.waitForFlight(workQueuedFlightId, null, null);

    flightState = stairway.getFlightState(workQueuedFlightId);
    assertThat(
        "4th flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    flightState = stairway.getFlightState(runningFlightId);
    assertThat(
        "1st flight succeeded", flightState.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
  }
}
