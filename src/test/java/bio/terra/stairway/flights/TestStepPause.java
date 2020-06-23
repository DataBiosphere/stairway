package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.fixtures.TestUtil;

public class TestStepPause implements Step {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    TestUtil.sleepPause();
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    TestUtil.sleepPause();
    return StepResult.getStepResultSuccess();
  }
}
