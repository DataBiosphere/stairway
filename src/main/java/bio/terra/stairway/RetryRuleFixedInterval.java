package bio.terra.stairway;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryRuleFixedInterval implements RetryRule {
  private static final Logger logger = LoggerFactory.getLogger(RetryRule.class);

  // Fixed parameters
  private final int intervalSeconds;
  private final int maxCount;

  // Initialized parameters
  private int retryCount;

  /**
   * Sleep for fixed intervals a fixed number of times.
   *
   * @param intervalSeconds - number of seconds to sleep
   * @param maxCount - number of times to retry
   */
  public RetryRuleFixedInterval(int intervalSeconds, int maxCount) {
    this.intervalSeconds = intervalSeconds;
    this.maxCount = maxCount;
    initialize();
  }

  @Override
  public void initialize() {
    retryCount = 0;
  }

  @Override
  public boolean retrySleep() throws InterruptedException {
    if (retryCount >= maxCount) {
      logger.info("Retry rule fixed: try {} of {} - not retrying", retryCount, maxCount);
      return false;
    }

    logger.info(
        "Retry rule fixed: try {} of {} - retrying after sleep of {} seconds",
        retryCount,
        maxCount,
        intervalSeconds);

    TimeUnit.SECONDS.sleep(intervalSeconds);
    retryCount++;
    return true;
  }
}
