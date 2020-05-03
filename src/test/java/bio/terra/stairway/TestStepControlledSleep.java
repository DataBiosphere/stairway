package bio.terra.stairway;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepControlledSleep implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepControlledSleep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    int stopSleepValue = inputParameters.get(MapKey.CONTROLLER_VALUE, Integer.class);

    while (TestStopController.getControl() != stopSleepValue) {
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
