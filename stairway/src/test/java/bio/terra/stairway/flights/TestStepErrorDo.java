package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class TestStepErrorDo implements Step {
  @Override
  public StepResult doStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new IllegalArgumentException("Initial error"));
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // no DO action
    return StepResult.getStepResultSuccess();
  }
}
