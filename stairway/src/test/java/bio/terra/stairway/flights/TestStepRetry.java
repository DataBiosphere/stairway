package bio.terra.stairway.flights;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepRetry implements Step {
  private Logger logger = LoggerFactory.getLogger(TestStepRetry.class);

  private int timesToFail;
  private int timesFailed;

  public TestStepRetry(int timesToFail) {
    this.timesToFail = timesToFail;
    this.timesFailed = 0;
  }

  @Override
  public StepResult doStep(FlightContext context) throws RetryException {
    logger.debug("TestStepRetry - timesFailed=" + timesFailed + " timesToFail=" + timesToFail);

    if (timesFailed < timesToFail) {
      timesFailed++;
      logger.debug(" - failure_retry");
      throw new RetryException("step retry failed");
    }
    logger.debug(" - success");
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
