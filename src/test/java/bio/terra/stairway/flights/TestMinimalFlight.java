package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class TestMinimalFlight extends Flight {

  public TestMinimalFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new NoopStep());
  }

  private static class NoopStep implements Step {
    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
      return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
      return StepResult.getStepResultSuccess();
    }
  }
}
