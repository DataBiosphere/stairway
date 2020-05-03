package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepForLoopUndo implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepForLoopUndo.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    int startCounter = inputParameters.get(MapKey.COUNTER_START, Integer.class);
    int endCounter = inputParameters.get(MapKey.COUNTER_END, Integer.class);
    int stopCounter = inputParameters.get(MapKey.COUNTER_STOP, Integer.class);

    FlightMap workingMap = context.getWorkingMap();
    Integer currentCounter = workingMap.get(MapKey.COUNTER, Integer.class);
    if (currentCounter == null) {
      logger.debug("Start of for loop");
      currentCounter = startCounter;
    }

    // Do processing for current counter
    logger.debug("For " + startCounter + " to " + endCounter + " processing at " + currentCounter);

    if (stopCounter == currentCounter) {
      logger.debug("For loop error");
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new IllegalStateException("Error!"));
    } else {
      logger.debug("For loop keep going");
    }

    currentCounter = currentCounter + 1;
    workingMap.put(MapKey.COUNTER, currentCounter);

    if (currentCounter >= endCounter) {
      logger.debug("End of for loop");
      return StepResult.getStepResultSuccess();
    }

    return new StepResult(StepStatus.STEP_RESULT_RERUN);
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    int startCounter = inputParameters.get(MapKey.COUNTER_START, Integer.class);
    int endCounter = inputParameters.get(MapKey.COUNTER_END, Integer.class);

    FlightMap workingMap = context.getWorkingMap();
    Integer currentCounter = workingMap.get(MapKey.COUNTER, Integer.class);
    if (currentCounter == null) {
      logger.debug("Loop never got started; nothing to undo");
      return StepResult.getStepResultSuccess();
    }

    // Do processing for current counter
    logger.debug(
        "Undo for " + startCounter + " to " + endCounter + " processing at " + currentCounter);

    currentCounter = currentCounter - 1;
    workingMap.put(MapKey.COUNTER, currentCounter);

    if (currentCounter < startCounter) {
      logger.debug("Start of for loop - all undone");
      return StepResult.getStepResultSuccess();
    }

    return new StepResult(StepStatus.STEP_RESULT_RERUN);
  }
}
