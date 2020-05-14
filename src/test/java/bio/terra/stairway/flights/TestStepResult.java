package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.fixtures.MapKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepResult implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepResult.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info(
        "ResultStep - Flight: "
            + context.getFlightId()
            + "; stairway: "
            + context.getStairway().getStairwayName());

    FlightMap inputParameters = context.getInputParameters();
    String resultValue = inputParameters.get(MapKey.RESULT, String.class);

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(MapKey.RESULT, resultValue);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
