package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import bio.terra.stairway.fixtures.FileQueue;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.fixtures.TestStairwayBuilder.UseQueue;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightControlledSleep;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("connected")
public class QueueFlightTest {
  private static final int QUEUE_SETTLE_SECONDS = 3;
  private final Logger logger = LoggerFactory.getLogger(QueueFlightTest.class);

  private Stairway stairway;

  @AfterEach
  public void cleanup() throws Exception {
    if (stairway != null) {
      stairway.terminate(5, TimeUnit.SECONDS);
      stairway = null;
    }
  }

  @Test
  public void queueFlightTest() throws Exception {
    // Submit directly to queue and make sure we end up in the right state
    stairway = new TestStairwayBuilder()
        .name("queueFlightTest")
        .testHookCount(3)
        .useQueue(UseQueue.MAKE_QUEUE)
        .build();

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

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:READY",
            "2:stateTransition:READY",
            "3:stateTransition:READY",
            "1:stateTransition:QUEUED",
            "2:stateTransition:QUEUED",
            "3:stateTransition:QUEUED",
            "1:stateTransition:RUNNING",
            "2:stateTransition:RUNNING",
            "3:stateTransition:RUNNING",
            "1:startFlight",
            "2:startFlight",
            "3:startFlight",
            "1:flightHook:startFlight",
            "2:flightHook:startFlight",
            "3:flightHook:startFlight",
            "1:startStep",
            "2:startStep",
            "3:startStep",
            "1:stepHook:startStep",
            "2:stepHook:startStep",
            "3:stepHook:startStep",
            "1:endStep",
            "2:endStep",
            "3:endStep",
            "1:stepHook:endStep",
            "2:stepHook:endStep",
            "3:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "2:stateTransition:SUCCESS",
            "3:stateTransition:SUCCESS",
            "1:endFlight",
            "2:endFlight",
            "3:endFlight",
            "1:flightHook:endFlight",
            "2:flightHook:endFlight",
            "3:flightHook:endFlight"));
  }

  @Test
  public void admissionControlTest() throws Exception {
    // We create a one thread work queue with 0 tolerance for queued flights.
    // Put one flight in that pauses.
    // Put another flight in. It should arrive in the QUEUED state
    String projectId = TestUtil.getEnvVar("GOOGLE_CLOUD_PROJECT", null);
    assertNotNull(projectId);

    QueueInterface workQueue = FileQueue.makeFileQueue("admissionControl");

    DataSource dataSource = TestUtil.makeDataSource();
    stairway =
        new StairwayBuilder()
            .stairwayName("admissionControlTest")
            .workQueue(workQueue)
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
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    // The second flight should be RUNNING, but be queued in the thread pool
    String threadQueuedFlightId = "threadQueuedFlight";
    stairway.submit(threadQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    FlightState flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight queued to thread pool ",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The third flight queued to the work queue, but run immediately, since there is an
    // outstanding pull request.
    String workQueuedRunFlightId = "workRunQueuedFlight";
    stairway.submit(workQueuedRunFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight run from work queue",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The fourth flight is queued to the work queue, and not run, since the queue depth
    // stops the listener from trying to pull more from the work queue.
    String workQueuedFlightId = "workQueuedFlight";
    stairway.submit(workQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

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
    QueueInterface workQueue = FileQueue.makeFileQueue("supplyQueue");

    stairway = new StairwayBuilder()
            .stairwayName("admissionControlTest")
            .workQueue(workQueue)
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
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    // The second flight should get pulled from the queue and be queued in the thread pool.
    String threadQueuedFlightId = "threadQueuedFlight";
    stairway.submitToQueue(threadQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    FlightState flightState = stairway.getFlightState(threadQueuedFlightId);
    assertThat(
        "2nd flight queued to thread pool ",
        flightState.getFlightStatus(),
        equalTo(FlightStatus.RUNNING));

    // The third flight queued to the work queue and not be run
    String workQueuedRunFlightId = "workQueuedFlight1";
    stairway.submitToQueue(workQueuedRunFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

    flightState = stairway.getFlightState(workQueuedRunFlightId);
    assertThat(
        "3rd flight In work queue", flightState.getFlightStatus(), equalTo(FlightStatus.QUEUED));

    // The fourth flight is queued to the work queue, and not run
    String workQueuedFlightId = "workQueuedFlight2";
    stairway.submitToQueue(workQueuedFlightId, TestFlightControlledSleep.class, inputs);
    TimeUnit.SECONDS.sleep(QUEUE_SETTLE_SECONDS);

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
