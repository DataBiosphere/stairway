package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.fixtures.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepIncrementStopUndo implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepIncrementStopUndo.class);

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputs = context.getInputParameters();
    FlightMap workingMap = context.getWorkingMap();

    Integer value = workingMap.get("value", Integer.class);
    if (value == null) {
      // Value hasn't been set yet, so we set it to initial value
      value = inputs.get("initialValue", Integer.class);
    }
    logger.debug("TestStepIncrementStopUndo - do - start value is: " + value);

    value = value + 1;
    workingMap.put("value", value);
    logger.debug("TestStepIncrementStopUndo - do - end value is: " + value);

    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new IllegalStateException("Test Error"));
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Optionally stop here
    TestUtil.sleepPause();

    FlightMap workingMap = context.getWorkingMap();
    Integer value = workingMap.get("value", Integer.class);
    logger.debug("TestStepIncrementStopUndo - undo - start value is: " + value);

    if (value != null) {
      value = value - 1;
      workingMap.put("value", value);
      logger.debug("TestStepIncrementStopUndo - undo - end value is: " + value);
    }
    return StepResult.getStepResultSuccess();
  }
}
