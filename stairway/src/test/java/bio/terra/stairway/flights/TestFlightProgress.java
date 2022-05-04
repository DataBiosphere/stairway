package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlightProgress extends Flight {
  private final Logger logger = LoggerFactory.getLogger(TestFlightProgress.class);

  public TestFlightProgress(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    String meter1 = inputParameters.get(MapKey.PROGRESS_NAME1, String.class);
    String meter2 = inputParameters.get(MapKey.PROGRESS_NAME2, String.class);

    addStep(new TestStepProgressLoop(meter1, logger));
    addStep(new TestStepProgressLoop(meter2, logger));
  }

  public static class TestStepProgressLoop implements Step {
    private final String meterName;
    private final Logger logger;

    TestStepProgressLoop(String meterName, Logger logger) {
      this.meterName = meterName;
      this.logger = logger;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
      FlightMap inputParameters = context.getInputParameters();
      long startCounter = inputParameters.get(MapKey.COUNTER_START, Long.class);
      long stopCounter = inputParameters.get(MapKey.COUNTER_STOP, Long.class);

      for (long i = startCounter; i <= stopCounter; i++) {
        logger.info("Setting meter {} to {} of {}", meterName, i, stopCounter);
        context.setProgressMeter(meterName, i, stopCounter);
        while (TestPauseController.getControl() != i) {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
      return StepResult.getStepResultSuccess();
    }
  }
}
