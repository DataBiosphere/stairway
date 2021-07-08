package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.fixtures.TestPauseController;

public class TestStepTriggerUndo implements Step {

  @Override
  public StepResult doStep(FlightContext context) {
    // This step sets the stop controller to 0 to cause the
    // stop step to sleep. Then it returns a fatal error.
    TestPauseController.setControl(0);
    throw new RuntimeException("TestStepTriggerUndo");
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
