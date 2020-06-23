package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepIncrement implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepIncrement.class);

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputs = context.getInputParameters();
    FlightMap workingMap = context.getWorkingMap();

    Integer value = workingMap.get("value", Integer.class);
    if (value == null) {
      // Value hasn't been set yet, so we set it to initial value
      value = inputs.get("initialValue", Integer.class);
    }
    logger.debug("TestStepIncrement - do - start value is: " + value);

    value = value + 1;
    workingMap.put("value", value);
    logger.debug("TestStepIncrement - do - end value is: " + value);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    Integer value = workingMap.get("value", Integer.class);
    logger.debug("TestStepIncrement - undo - start value is: " + value);

    if (value != null) {
      value = value - 1;
      workingMap.put("value", value);
      logger.debug("TestStepIncrement - undo - end value is: " + value);
    }

    return StepResult.getStepResultSuccess();
  }
}
