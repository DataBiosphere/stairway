package bio.terra.stairway.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightRecovery;
import bio.terra.stairway.flights.TestFlightRecoveryUndo;
import bio.terra.stairway.flights.TestFlightRecoveryUndoSwitch;
import bio.terra.stairway.flights.TestFlightStop;
import java.util.concurrent.TimeUnit;
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
    Stairway stairway1 = new TestStairwayBuilder().name(stairwayName).build();

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
    // sleep. We reuse the stairway name to make sure that replacement by the same name works.
    TestPauseController.setControl(1);
    Stairway stairway2 =
        new TestStairwayBuilder().name(stairwayName).continuing(true).doRecoveryCheck(true).build();

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
    final String stairwayName = "recoverySuccessTest";
    Stairway stairway1 = new TestStairwayBuilder().name(stairwayName).build();

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
    Stairway stairway2 = new TestStairwayBuilder().name(stairwayName).continuing(true).build();

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
    // Start with a clean and shiny database environment.
    final String stairwayName = "recoveryUndoSwitchTest";
    Stairway stairway1 = new TestStairwayBuilder().name(stairwayName).build();

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
    Stairway stairway2 = new TestStairwayBuilder().name(stairwayName).continuing(true).build();

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
    // Use the implementation class so we can call recovery
    StairwayImpl stairway = (StairwayImpl) new TestStairwayBuilder().name("stopStepResult").build();

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
  public void recoverOneTest() throws Exception {
    final String stairway1Name = "recoverOneTestOne";
    final String stairway2Name = "recoverOneTestTwo";

    // The idea here is to start two stairways, run a flight on one that pauses.
    // Then from the second, use the recoverStairway() method to recover the flight
    // and continue it. This test is similar to the startup recover test, but
    // exercises the case where the failure of one stairway instance is detected
    // and recovered by another stairway.

    Stairway stairway1 = new TestStairwayBuilder().name(stairway1Name).build();
    Stairway stairway2 =
        new TestStairwayBuilder()
            .name(stairway2Name)
            .continuing(true)
            .existingStairwaysAreAlive(true)
            .build();

    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    TestPauseController.setControl(0);
    String flightId = "recoverOneTest";
    stairway1.submit(flightId, TestFlightRecovery.class, inputs);

    // Allow time for the flight thread to go to sleep
    TimeUnit.SECONDS.sleep(5);
    assertThat(TestUtil.isDone(stairway1, flightId), is(equalTo(false)));

    // Pretend we notice that stairway1 is down. We recover stairway1.
    // That should rerun the "pause" step of TestFlightRecovery class, this
    // time with control set to 1 it will complete.
    TestPauseController.setControl(1);
    stairway2.recoverStairway(stairway1Name);

    // Wait for recovery to complete
    stairway2.waitForFlight(flightId, null, null);
    FlightState result = stairway2.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(equalTo(2)));
  }
}
