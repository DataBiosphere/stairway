package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.StairwayExecutionException;

class TriggerUndoStep implements Step {

  @Override
  public StepResult doStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new StairwayExecutionException("TestTriggerUndoStep"));
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
