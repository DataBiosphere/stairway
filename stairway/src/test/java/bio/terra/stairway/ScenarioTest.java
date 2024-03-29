package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlight;
import bio.terra.stairway.flights.TestFlightQuietDown;
import bio.terra.stairway.flights.TestFlightRerun;
import bio.terra.stairway.flights.TestFlightRerunUndo;
import bio.terra.stairway.flights.TestFlightUndo;
import bio.terra.stairway.flights.TestFlightWait;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class ScenarioTest {
  private final Logger logger = LoggerFactory.getLogger(ScenarioTest.class);
  private Stairway stairway;

  @BeforeEach
  public void setup() throws Exception {
    stairway = new TestStairwayBuilder().testHookCount(1).build();
  }

  @Test
  public void simpleTest() throws Exception {
    // Generate a unique filename
    String filename = makeFilename();
    logger.debug("Filename: " + filename);

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    inputParameters.put("filename", filename);
    inputParameters.put("text", "testing 1 2 3");

    String flightId = "simpleTest";
    stairway.submit(flightId, TestFlight.class, inputParameters);
    logger.debug("Submitted flight id: " + flightId);

    // Test for done
    boolean done = TestUtil.isDone(stairway, flightId);
    logger.debug("Flight done: " + done);

    // Wait for done
    FlightState result = stairway.waitForFlight(flightId, null, null);
    assertThat(result.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
    assertFalse(result.getException().isPresent());

    String badFlightId = UUID.randomUUID().toString();
    try {
      stairway.waitForFlight(badFlightId, null, null);
    } catch (FlightNotFoundException ex) {
      assertThat(ex.getMessage(), containsString(badFlightId));
    }

    // Validate the hook log
    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testFileExists() throws Exception {
    // Generate a filename and create the file
    String filename = makeExistingFile();

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    inputParameters.put("filename", filename);
    inputParameters.put("text", "testing 1 2 3");

    String flightId = "fileTest";
    stairway.submit(flightId, TestFlight.class, inputParameters);

    // Poll waiting for done
    while (!TestUtil.isDone(stairway, flightId)) {
      TimeUnit.SECONDS.sleep(1);
    }

    // Handle results
    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), equalTo(FlightStatus.ERROR));
    assertTrue(result.getException().isPresent());

    // The error text thrown by TestStepExistence
    assertThat(result.getException().get().getMessage(), containsString("already exists"));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testUndo() throws Exception {
    // The plan is:
    // > pre-create abcd.txt
    // > random file
    //  - step 1 file exists random file
    //  - step 2 create random file
    //  - step 3 file exists pre-created file (will fail)
    //  - step 4 create pre-created file (should not get here)

    // Generate a filename and create the file
    String existingFilename = makeExistingFile();

    // Generate non-existent filename
    String filename = makeFilename();

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    inputParameters.put("filename", filename);
    inputParameters.put("existingFilename", existingFilename);
    inputParameters.put("text", "testing 1 2 3");

    String flightId = "undoTest";
    stairway.submit(flightId, TestFlightUndo.class, inputParameters);

    // Wait for done
    FlightState result = stairway.waitForFlight(flightId, null, null);
    assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));
    assertTrue(result.getException().isPresent());
    assertThat(result.getException().get().getMessage(), containsString("already exists"));

    // We expect the non-existent filename to have been deleted
    File file = new File(filename);
    assertFalse(file.exists());

    // We expect the existent filename to still be there
    file = new File(existingFilename);
    assertTrue(file.exists());

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testQuietDown() throws Exception {
    String inResult = "quieted down and woke up";
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(MapKey.CONTROLLER_VALUE, 1);
    inputParameters.put(MapKey.RESULT, inResult);

    TestPauseController.setControl(0);
    String flightId = stairway.createFlightId();

    stairway.submit(flightId, TestFlightQuietDown.class, inputParameters);
    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    // Quiet down - don't wait long; we want control so we can unblock the thread!
    boolean quietYet = stairway.quietDown(1, TimeUnit.SECONDS);
    assertFalse(quietYet, "Not quiet yet");

    // Wake up the thread; it should exit into READY state
    TestPauseController.setControl(1);
    // Allow time for the flight thread to wake up and exit
    TimeUnit.SECONDS.sleep(5);

    FlightState state = stairway.getFlightState(flightId);
    assertThat("State is ready", state.getFlightStatus(), equalTo(FlightStatus.READY));
    assertNull(state.getStairwayId(), "Flight is unowned");

    String stairwayName = stairway.getStairwayName();

    stairway.terminate(5, TimeUnit.SECONDS);
    // Create a new stairway instance to test flight continuity over terminate-recover
    stairway =
        new TestStairwayBuilder()
            .name(stairwayName)
            .continuing(true)
            .doRecoveryCheck(true)
            .flightId(flightId)
            .testHookCount(1)
            .build();

    stairway.waitForFlight(flightId, null, null);
    state = stairway.getFlightState(flightId);
    assertThat("State is success", state.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    FlightMap resultMap = state.getResultMap().orElse(null);
    assertNotNull(resultMap, "result map is present");
    String outResult = resultMap.get(MapKey.RESULT, String.class);
    assertThat("result set properly", outResult, equalTo(inResult));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:READY",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testWait() throws Exception {
    String inResult = "wait and merged";
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(MapKey.RESULT, inResult);

    String flightId = stairway.createFlightId();

    stairway.submit(flightId, TestFlightWait.class, inputParameters);
    // Allow time for the flight thread to start up and yield
    TimeUnit.SECONDS.sleep(5);

    FlightState state = stairway.getFlightState(flightId);
    assertThat("State is waiting", state.getFlightStatus(), equalTo(FlightStatus.WAITING));
    assertNull(state.getStairwayId(), "Flight is unowned");

    boolean resumedFlight = stairway.resume(flightId);
    assertTrue(resumedFlight, "successfully resumed the flight");
    stairway.waitForFlight(flightId, null, null);

    state = stairway.getFlightState(flightId);
    assertThat("State is success", state.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    FlightMap resultMap = state.getResultMap().orElse(null);
    assertNotNull(resultMap, "result map is present");
    String outResult = resultMap.get(MapKey.RESULT, String.class);
    assertThat("result set properly", outResult, equalTo(inResult));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:WAITING",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testTerminate() throws Exception {
    String inResult = "terminated cleanly";
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(MapKey.CONTROLLER_VALUE, 1);
    inputParameters.put(MapKey.RESULT, inResult);

    TestPauseController.setControl(0);
    String flightId = stairway.createFlightId();

    stairway.submit(flightId, TestFlightQuietDown.class, inputParameters);
    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    String stairwayName = stairway.getStairwayName();
    stairway.terminate(5, TimeUnit.SECONDS);
    // Create a new stairway and check flight recovery
    stairway =
        new TestStairwayBuilder()
            .name(stairwayName)
            .continuing(true)
            .doRecoveryCheck(true)
            .flightId(flightId)
            .testHookCount(1)
            .build();

    TestPauseController.setControl(1); // wake it up
    stairway.waitForFlight(flightId, null, null);

    FlightState state = stairway.getFlightState(flightId);
    assertThat("State is success", state.getFlightStatus(), equalTo(FlightStatus.SUCCESS));

    FlightMap resultMap = state.getResultMap().orElse(null);
    assertNotNull(resultMap, "result map is present");
    String outResult = resultMap.get(MapKey.RESULT, String.class);
    assertThat("result set properly", outResult, equalTo(inResult));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:READY",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testRerunSimple() throws Exception {
    String inResult = "rerun is simple";
    FlightMap inputParameters = new FlightMap();
    int counterEnd = 5;
    inputParameters.put(MapKey.COUNTER_START, 0);
    inputParameters.put(MapKey.COUNTER_END, counterEnd);
    inputParameters.put(MapKey.RESULT, inResult);

    String flightId = stairway.createFlightId();
    stairway.submit(flightId, TestFlightRerun.class, inputParameters);
    FlightState state = stairway.waitForFlight(flightId, null, null);
    assertThat("State is success", state.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
    FlightMap resultMap = state.getResultMap().orElse(null);
    assertNotNull(resultMap, "result map is present");
    String outResult = resultMap.get(MapKey.RESULT, String.class);
    assertThat("result set properly", outResult, equalTo(inResult));
    Integer counter = resultMap.get(MapKey.COUNTER, Integer.class);
    assertThat("counter is counterend", counter, equalTo(counterEnd));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testRerunTerminate() throws Exception {
    String inResult = "rerun interrupted";
    FlightMap inputParameters = new FlightMap();
    int counterEnd = 5;
    inputParameters.put(MapKey.COUNTER_START, 0);
    inputParameters.put(MapKey.COUNTER_END, counterEnd);
    inputParameters.put(MapKey.COUNTER_STOP, 3);
    inputParameters.put(MapKey.RESULT, inResult);

    TestPauseController.setControl(0); // have the flight sleep at COUNTER_STOP
    String flightId = stairway.createFlightId();
    stairway.submit(flightId, TestFlightRerun.class, inputParameters);
    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    String stairwayName = stairway.getStairwayName();
    stairway.terminate(5, TimeUnit.SECONDS);

    stairway =
        new TestStairwayBuilder()
            .name(stairwayName)
            .continuing(true)
            .doRecoveryCheck(true)
            .flightId(flightId)
            .testHookCount(1)
            .build();

    TestPauseController.setControl(1); // prevent the flight from re-sleeping
    stairway.waitForFlight(flightId, null, null);

    FlightState state = stairway.getFlightState(flightId);
    assertThat("State is success", state.getFlightStatus(), equalTo(FlightStatus.SUCCESS));
    FlightMap resultMap = state.getResultMap().orElse(null);
    assertNotNull(resultMap, "result map is present");
    String outResult = resultMap.get(MapKey.RESULT, String.class);
    assertThat("result set properly", outResult, equalTo(inResult));
    Integer counter = resultMap.get(MapKey.COUNTER, Integer.class);
    assertThat("counter is counterend", counter, equalTo(counterEnd));

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:READY",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:SUCCESS",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  @Test
  public void testRerunUndo() throws Exception {
    String inResult = "rerun is undoable";
    int counterStart = 0;
    int counterEnd = 5;

    for (int stopCounter = 0; stopCounter < counterEnd; stopCounter++) {
      logger.debug("TestRerunUndo - stop at " + stopCounter);
      FlightMap inputParameters = new FlightMap();
      inputParameters.put(MapKey.COUNTER_START, counterStart);
      inputParameters.put(MapKey.COUNTER_END, counterEnd);
      inputParameters.put(MapKey.RESULT, inResult);
      inputParameters.put(MapKey.COUNTER_STOP, stopCounter);
      String flightId = stairway.createFlightId();
      stairway.submit(flightId, TestFlightRerunUndo.class, inputParameters);
      FlightState state = stairway.waitForFlight(flightId, null, null);
      assertThat("State is error", state.getFlightStatus(), equalTo(FlightStatus.ERROR));
      FlightMap resultMap = state.getResultMap().orElse(null);
      assertNotNull(resultMap, "result map is present");
      String outResult = resultMap.get(MapKey.RESULT, String.class);
      assertNull(outResult, "result is not present");
      Integer counter = resultMap.get(MapKey.COUNTER, Integer.class);
      if (counter == null) {
        assertThat("counter stop is zero", stopCounter, equalTo(counterStart));
      } else {
        assertThat("counter is counterstart", counter, lessThan(counterStart));
      }
    }

    TestUtil.checkHookLog(
        Arrays.asList(
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight",
            "1:stateTransition:RUNNING",
            "1:startFlight",
            "1:flightHook:startFlight",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:startStep",
            "1:stepHook:startStep",
            "1:endStep",
            "1:stepHook:endStep",
            "1:stateTransition:ERROR",
            "1:endFlight",
            "1:flightHook:endFlight"));
  }

  private String makeExistingFile() throws Exception {
    // Generate a filename and create the file
    String existingFilename = makeFilename();
    PrintWriter writer = new PrintWriter(existingFilename, "UTF-8");
    writer.println("abcd");
    writer.close();
    logger.debug("Existing Filename: " + existingFilename);
    return existingFilename;
  }

  private String makeFilename() {
    return "/tmp/test." + UUID.randomUUID().toString() + ".txt";
  }

  @Test
  public void threadedTest() throws Exception {

    // Like simpleTest(), but with threads.  Spawn a set of threads using ThreadedTestRunnable,
    // which will launch flights, wait for completion, and cache results (and any exceptions
    // thrown).  We will join these threads and then assert expected results.

    List<ThreadedTestRunnable> runnableList = new ArrayList<>();
    List<Thread> threadList = new ArrayList<>();

    final int threadCount = Runtime.getRuntime().availableProcessors();

    // Phaser is used as a simple barrier to try to sync flight submissions.
    Phaser phaser = new Phaser(threadCount);

    for (int i = 0; i < threadCount; ++i) {
      ThreadedTestRunnable runnable = new ThreadedTestRunnable(phaser, i);
      runnableList.add(runnable);

      Thread thread = new Thread(runnable);
      thread.start();
      threadList.add(thread);
    }

    // Wait for all threads.
    for (Thread thread : threadList) {
      thread.join();
    }

    // Assert all results (throwing any exceptions caught in threads along the way).
    for (ThreadedTestRunnable runnable : runnableList) {
      runnable.throwExceptionIfPresent();
      assertThat(runnable.getResult().getFlightStatus(), equalTo(FlightStatus.SUCCESS));
      assertFalse(runnable.getResult().getException().isPresent());
    }
  }

  /**
   * Class to enable creating Flights and waiting for their completion in background threads,
   * caching results and any thrown exceptions for inspection in the main test thread.
   */
  private class ThreadedTestRunnable implements Runnable {

    private final int index;
    private final String filename;
    Phaser phaser;
    Exception exception;
    FlightState result;

    ThreadedTestRunnable(Phaser phaser, int index) {
      this.phaser = phaser;
      this.index = index;
      this.filename = makeFilename();
    }

    @Override
    public void run() {

      // This basically implements the same logic as simpleTest in a background thread, holding
      // results and any caught exceptions for inspection by the main thread.  A synchronization
      // point is also injected via the use of a Phaser to introduce a barrier before all threads
      // submit their flights, with the intent of triggering timestamp collisions to regression test
      // the fix for PF-785.

      try {
        // Submit the test flight
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("filename", filename);
        inputParameters.put("text", "testing 1 2 3");

        logger.debug(
            "Waiting to launch TestFlight '{}' with filename '{}'", getFlightId(), filename);

        // Phaser is used here as a simple barrier to try to induce duplicate timestamps.  All
        // threads wait on the barrier until the last one reaches it.
        phaser.arriveAndAwaitAdvance();
        stairway.submit(getFlightId(), TestFlight.class, inputParameters);

        logger.debug("Waiting for flight {}", getFlightId());
        result = stairway.waitForFlight(getFlightId(), null, null);
      } catch (Exception ex) {
        logger.error(ex.getMessage());
        exception = ex;
      }
    }

    public String getFlightId() {
      return String.format("threadedTest%02X", index);
    }

    public String getFilename() {
      return filename;
    }

    public void throwExceptionIfPresent() throws Exception {
      if (exception != null) {
        throw exception;
      }
    }

    public FlightState getResult() {
      return result;
    }
  }
}
