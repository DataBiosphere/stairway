package bio.terra.stairway;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepSleep implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepSleep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info(
        "SleepStep - Flight: "
            + context.getFlightId()
            + "; stairway: "
            + context.getStairway().getStairwayName());

    FlightMap inputParameters = context.getInputParameters();
    int sleepSeconds = inputParameters.get(MapKey.SLEEP_SECONDS, Integer.class);
    TimeUnit.SECONDS.sleep(sleepSeconds);

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(MapKey.RESULT, "sleep step woke up");
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
