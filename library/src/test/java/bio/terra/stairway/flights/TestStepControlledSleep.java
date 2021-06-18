package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This step sleeps until the control value from TestPauseController
// matches the input parameter controller value.
public class TestStepControlledSleep implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepControlledSleep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    int stopSleepValue = inputParameters.get(MapKey.CONTROLLER_VALUE, Integer.class);

    while (TestPauseController.getControl() != stopSleepValue) {
      TimeUnit.SECONDS.sleep(1);
    }

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(MapKey.RESULT, "sleep step woke up");
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
