package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepWait implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepWait.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    return new StepResult(StepStatus.STEP_RESULT_WAIT);
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
