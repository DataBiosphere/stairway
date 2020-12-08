package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.fixtures.TestUtil;
import bio.terra.stairway.flights.TestFlightRestarting;
import java.util.HashMap;
import java.util.Map;
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
  public void failAtStepsRetryable() throws Exception {
    final String stairwayName = "failAtStepsRetryable";
    String flightId = "failAtStepsRetryable";

    Map<Integer, StepStatus> failures = new HashMap<>();
    failures.put(2, StepStatus.STEP_RESULT_FAILURE_RETRY);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().failAtSteps(failures).build();

    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    stairway.submitWithDebugInfo(flightId, TestFlightRestarting.class, inputs, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(Matchers.equalTo(true)));

    FlightState result = stairway.getFlightState(flightId);
    // We should have an error but not a Dismal Failure.
    assertThat(result.getFlightStatus(), is(Matchers.equalTo(FlightStatus.ERROR)));
    assertTrue(result.getResultMap().isPresent());
    // Should have properly decremented.
    Integer value = result.getResultMap().get().get("value", Integer.class);
    assertThat(value, is(Matchers.equalTo(0)));
  }

  @Test
  public void failAtStepsFatal() throws Exception {
    final String stairwayName = "failAtStepsFatal";
    String flightId = "failAtStepsFatal";

    Map<Integer, StepStatus> failures = new HashMap<>();
    failures.put(2, StepStatus.STEP_RESULT_FAILURE_FATAL);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().failAtSteps(failures).build();

    Stairway stairway = TestUtil.setupStairway(stairwayName, false);
    FlightMap inputs = new FlightMap();

    Integer initialValue = 0;
    inputs.put("initialValue", initialValue);

    stairway.submitWithDebugInfo(flightId, TestFlightRestarting.class, inputs, true, debugInfo);

    // Allow time for the flight thread to run
    TimeUnit.SECONDS.sleep(5);

    assertThat(TestUtil.isDone(stairway, flightId), is(Matchers.equalTo(true)));

    FlightState result = stairway.getFlightState(flightId);
    // We should have an error but not a Dismal Failure.
    assertThat(result.getFlightStatus(), is(Matchers.equalTo(FlightStatus.ERROR)));
    // Not sure what the right behavior is here.
    //assertTrue(result.getException().isPresent());
  }
}
