package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlight;
import bio.terra.stairway.flights.TestFlightMultiStepRetry;
import bio.terra.stairway.flights.TestFlightRestarting;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class DebugInfoTest {

  private Logger logger = LoggerFactory.getLogger(DebugInfoTest.class);

  /**
   * Ensure that the entire flight executes when debug info is set to true. TODO(tovanadler): Ensure
   * the proper DB transitions have happened with a hook on DlightDao
   *
   * @throws Exception
   */
  @Test
  public void restartEachStepTrueTest() throws Exception {
    final String stairwayName = "restartEachStepTrueTest";
    String flightId = "restartEachStepTrueTest";

    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().restartEachStep(true).build();
    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    stairway.submitWithDebugInfo(flightId, TestFlightRestarting.class, inputs, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(Matchers.equalTo(true)));

    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(Matchers.equalTo(FlightStatus.SUCCESS)));
    assertTrue(result.getResultMap().isPresent());
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(Matchers.equalTo(3)));
  }

  @Test
  public void lastStepFailureTrueTest() throws Exception {
    final String stairwayName = "lastStepFailureTrueTest";
    String flightId = "lastStepFailureTrueTest";

    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().lastStepFailure(true).build();
    Stairway stairway = TestUtil.setupStairwayWithHooks(stairwayName, false, 1);

    // Submit the test flight
    FlightMap inputParameters = new FlightMap();
    inputParameters.put("filename", makeFilename());
    inputParameters.put("text", "testing 1 2 3");

    stairway.submitWithDebugInfo(flightId, TestFlight.class, inputParameters, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(true));

    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));

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
  public void failAtStepsRetryable() throws Exception {
    final String stairwayName = "failAtStepsRetryable";
    String flightId = "failAtStepsRetryable";

    Map<Integer, StepStatus> failures = new HashMap<>();
    failures.put(0, StepStatus.STEP_RESULT_FAILURE_RETRY);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().failAtSteps(failures).build();

    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightMap inputs = new FlightMap();
    String filename = makeFilename();
    inputs.put("filename", filename);
    inputs.put("text", "testing 1 2 3");
    inputs.put("retryType", "fixed");
    inputs.put("failCount", 2);
    inputs.put("intervalSeconds", 2);
    inputs.put("maxCount", 4);
    stairway.submitWithDebugInfo(flightId, TestFlightMultiStepRetry.class, inputs, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(Matchers.equalTo(true)));
    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(Matchers.equalTo(FlightStatus.SUCCESS)));
    File file = new File(filename);
    assertTrue(file.exists());
  }

  @Test
  public void failAtStepsFatal() throws Exception {
    final String stairwayName = "failAtStepsFatal";
    String flightId = "failAtStepsFatal";

    Map<Integer, StepStatus> failures = new HashMap<>();
    failures.put(0, StepStatus.STEP_RESULT_FAILURE_FATAL);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().failAtSteps(failures).build();

    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightMap inputs = new FlightMap();
    String filename = makeFilename();
    inputs.put("filename", filename);
    inputs.put("text", "testing 1 2 3");
    inputs.put("retryType", "fixed");
    inputs.put("failCount", 2);
    inputs.put("intervalSeconds", 2);
    inputs.put("maxCount", 4);
    stairway.submitWithDebugInfo(flightId, TestFlightMultiStepRetry.class, inputs, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(Matchers.equalTo(true)));
    FlightState result = stairway.getFlightState(flightId);
    assertThat(result.getFlightStatus(), is(Matchers.equalTo(FlightStatus.ERROR)));
    assertTrue(result.getResultMap().isPresent());
    File file = new File(filename);
    assertFalse(file.exists());
  }

  private String makeFilename() {
    return "/tmp/test." + UUID.randomUUID().toString() + ".txt";
  }
}
