package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightRecovery;
import bio.terra.stairway.flights.TestFlightRecoveryUndo;
import bio.terra.stairway.flights.TestFlightRecoveryUndoSwitch;
import bio.terra.stairway.flights.TestFlightRecoveryUnrecoverableInput;
import bio.terra.stairway.flights.TestFlightRecoveryUnrecoverableWorkingMap;
import bio.terra.stairway.flights.TestFlightStop;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The recovery tests are a bit tricky to write, because we need to simulate a failure, create a new
 * stairway and successfully run recovery.
 *
 * <p>The first part of the solution is to build a way to stop a flight at a step to control where
 * the failure happens, and to not stop at that step the second time through. To do the
 * coordination, we make a singleton class TestStopController that holds a volatile variable. We
 * make an associated step class called TestStepStop that finds the stop controller, reads the
 * variable, and obeys the variable's instruction. Here are the cases: - stop controller = 0 means
 * to sit in a sleep loop forever (well, an hour) - stop controller = 1 means to skip sleeping
 *
 * <p>The success recovery test works like this:
 * <li>Set stop controller = 0
 * <li>Create stairway1 and launch a flight
 * <li>Flight runs steps up to the stop step
 * <li>Stop step goes to sleep At this point, the database looks like we have a partially complete,
 *     succeeding flight. Now we can test recovery:
 * <li>Set stop controller = 1
 * <li>Create stairway2 and do the three step startup, recovering the obsolete Stairway.
 * <li>Flight re-does the stop step. This time stop controller is 2 and we skip the sleeping
 * <li>Flight completes successfully At this point: we can evaluate the results of the recovered
 *     flight to make sure it worked right. When the test is torn down, the sleeping thread will
 *     record failure in the database, so you cannot use state there at this point for any
 *     validation.
 *
 *     <p>The undo recovery test works by introducing TestStepTriggerUndo that will set the stop
 *     controller from 0 and trigger undo. Then the TestStepStop will sleep on the undo path
 *     simulating a failure in that direction.
 */
@Tag("unit")
public class RecoveryTest {
  @Test
  public void successTest() throws Exception {
    final String stairwayName = "recoverySuccessTest";

    // Start with a clean and shiny database environment.
    Stairway stairway1 = TestUtil.setupStairway(stairwayName, false);

    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    TestPauseController.setControl(0);
    String flightId = "successTest";
    stairway1.submit(flightId, TestFlightRecovery.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway1, flightId), is(equalTo(false)));

    // Simulate a restart with a new thread pool and stairway. Set control so this one does not
    // sleep. We create the new stairway directly, rather than use TestUtil so we can validate the
    // process. We reuse the stairway name to make sure that replacement by the same name works.
    TestPauseController.setControl(1);
    DataSource dataSource = TestUtil.makeDataSource();
    Stairway stairway2 =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(null)
            .maxParallelFlights(2)
            .build();
    List<String> recordedStairways = stairway2.initialize(dataSource, false, false);
    assertThat("One obsolete stairway to recover", recordedStairways.size(), equalTo(1));
    String obsoleteStairway = recordedStairways.get(0);
    assertThat("Obsolete stairway has the right name", obsoleteStairway, equalTo(stairwayName));

    stairway2.recoverAndStart(recordedStairways);

    // Wait for recovery to complete
    stairway2.waitForFlight(flightId, null, null);
    FlightState result = stairway2.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(2)));
  }

  @Test
  public void undoTest() throws Exception {
    // Start with a clean and shiny database environment.
    Stairway stairway1 = TestUtil.setupStairway("recoverySuccessTest", false);

    FlightMap inputs = new FlightMap();
    Integer initialValue = 2;
    inputs.put("initialValue", initialValue);

    // We don't want to stop on the do path; the undo trigger will set the control to 0 and put the
    // flight to sleep
    TestPauseController.setControl(1);
    String flightId = "undoTest";
    stairway1.submit(flightId, TestFlightRecoveryUndo.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    // Simulate a restart with a new thread pool and stairway. Reset control so this one does not
    // sleep
    TestPauseController.setControl(1);
    Stairway stairway2 = TestUtil.setupStairway("recoverySuccessTest", true);

    // Wait for recovery to complete
    stairway2.waitForFlight(flightId, 5, 10);
    FlightState result = stairway2.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
    assertTrue(result.getException().isPresent());
    // The exception is thrown by TestStepTriggerUndo
    assertThat(result.getException().get().getMessage(), is(containsString("TestStepTriggerUndo")));

    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(2)));
  }

  @Test
  public void undoSwitchTest() throws Exception {
    // Test recovering at the point where we switch from doing to undoing.
    // We do this by causing one flight in one Stairway to error and have
    // its undo stop.
    // Then we recover the flight and make sure it works right.
    Stairway stairway1 = TestUtil.setupStairway("recoveryUndoSwitchTest", false);

    FlightMap inputs = new FlightMap();
    Integer initialValue = 5;
    inputs.put("initialValue", initialValue);

    // We stop the flight on the undo path
    TestPauseController.setControl(0);
    String flightId = "undoSwitchTest";
    stairway1.submit(flightId, TestFlightRecoveryUndoSwitch.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    // Simulate a restart with a new thread pool and stairway. Reset control so this one does not
    // sleep
    TestPauseController.setControl(1);
    Stairway stairway2 = TestUtil.setupStairway("recoveryUndoSwitchTest", true);

    // Wait for recovery to complete
    stairway2.waitForFlight(flightId, 5, 10);
    FlightState result = stairway2.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
    assertTrue(result.getException().isPresent());
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(5)));
  }

  @Test
  public void stopStepResultTest() throws Exception {
    Stairway stairway = TestUtil.setupStairway("stopStepResult", false);

    FlightMap inputs = new FlightMap();
    Integer initialValue = 10;
    inputs.put("initialValue", initialValue);

    String flightId = "stopStepResultTest";
    stairway.submit(flightId, TestFlightStop.class, inputs);

    // Allow time for the flight to stop
    TimeUnit.SECONDS.sleep(5);
    FlightState flightState = stairway.getFlightState(flightId);
    assertThat("Flight is READY", flightState.getFlightStatus(), equalTo(FlightStatus.READY));

    stairway.recoverReady();

    stairway.waitForFlight(flightId, 5, 10);
    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
    assertFalse(result.getException().isPresent());
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(12)));
  }

  @Test
  public void unrecoverableFlightTest() throws Exception {
    DataSource dataSource = TestUtil.makeDataSource();
    final String stairwayName = "recoveryUnrecoverableFlightTest";

    // Start with a clean and shiny database environment with enough threads for our test.
    Stairway stairway1 =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(null)
            .maxParallelFlights(3)
            .build();
    stairway1.recoverAndStart(stairway1.initialize(dataSource, false, false));

    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    // Submit one flight that will start, stop and then be recovered successfully.
    TestPauseController.setControl(0);
    String okFlightId = "okFlight";
    stairway1.submit(okFlightId, TestFlightRecovery.class, inputs);

    // Submit one flight that will be unable to be recovered because of inputs.
    String badInputFlightId = "badInputFlight";
    FlightMap badInputs = new FlightMap();
    badInputs.put("initialValue", initialValue);
    badInputs.put(TestFlightRecoveryUnrecoverableInput.INPUT_KEY, UUID.randomUUID());
    stairway1.submit(badInputFlightId, TestFlightRecoveryUnrecoverableInput.class, badInputs);

    // Submit one flight that will be unable to be recovered because of the working map value.
    String badWorkingMapFlightId = "badWorkingMapFlight";
    stairway1.submit(
        badWorkingMapFlightId, TestFlightRecoveryUnrecoverableWorkingMap.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    assertFalse(TestUtil.isDone(stairway1, okFlightId));
    assertFalse(TestUtil.isDone(stairway1, badInputFlightId));
    assertFalse(TestUtil.isDone(stairway1, badWorkingMapFlightId));

    // Simulate a restart with a new thread pool and stairway. Set control so this one does not
    // sleep. We create the new stairway directly, rather than use TestUtil so we can validate the
    // process. We reuse the stairway name to make sure that replacement by the same name works.
    TestPauseController.setControl(1);
    Stairway stairway2 =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(null)
            .maxParallelFlights(3)
            .build();
    List<String> recordedStairways = stairway2.initialize(dataSource, false, false);
    assertThat("One obsolete stairway to recover", recordedStairways, hasSize(1));
    String obsoleteStairway = recordedStairways.get(0);
    assertThat("Obsolete stairway has the right name", obsoleteStairway, equalTo(stairwayName));

    stairway2.recoverAndStart(recordedStairways);

    // Wait for recovery to complete
    stairway2.waitForFlight(okFlightId, 1, 20);
    FlightState result = stairway2.getFlightState(okFlightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(2)));

    // The unrecoverable flights are marked as having failed fatally.
    stairway2.waitForFlight(badInputFlightId, 1, 20);
    assertThat(
        stairway2.getFlightState(badInputFlightId).getFlightStatus(), equalTo(FlightStatus.FATAL));
    stairway2.waitForFlight(badWorkingMapFlightId, 1, 20);
    assertThat(
        stairway2.getFlightState(badWorkingMapFlightId).getFlightStatus(),
        equalTo(FlightStatus.FATAL));
  }

  @Test
  public void onlyUnrecoverableFlightTest() throws Exception {
    DataSource dataSource = TestUtil.makeDataSource();
    final String stairwayName = "recoveryOnlyUnrecoverableFlightTest";

    // Start with a clean and shiny database environment with enough threads for our test.
    Stairway stairway1 = TestUtil.setupStairway(stairwayName, false);

    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    // Submit one flight that will be unable to be recovered because of the working map value.
    TestPauseController.setControl(0);
    String badWorkingMapFlightId = "badWorkingMapFlight";
    stairway1.submit(
        badWorkingMapFlightId, TestFlightRecoveryUnrecoverableWorkingMap.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);

    assertFalse(TestUtil.isDone(stairway1, badWorkingMapFlightId));

    // Simulate a restart with a new thread pool and stairway. Set control so this one does not
    // sleep. We create the new stairway directly, rather than use TestUtil so we can validate the
    // process. We reuse the stairway name to make sure that replacement by the same name works.
    TestPauseController.setControl(1);
    Stairway stairway2 =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(null)
            .maxParallelFlights(3)
            .build();
    List<String> recordedStairways = stairway2.initialize(dataSource, false, false);
    assertThat("One obsolete stairway to recover", recordedStairways, hasSize(1));
    String obsoleteStairway = recordedStairways.get(0);
    assertThat("Obsolete stairway has the right name", obsoleteStairway, equalTo(stairwayName));

    stairway2.recoverAndStart(recordedStairways);

    // The unrecoverable flights are marked as having failed fatally, even when there was only
    // unrecoverable flights to recover.
    stairway2.waitForFlight(badWorkingMapFlightId, 1, 20);
    assertThat(
        stairway2.getFlightState(badWorkingMapFlightId).getFlightStatus(),
        equalTo(FlightStatus.FATAL));
  }
}
