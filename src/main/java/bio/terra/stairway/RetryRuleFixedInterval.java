package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
    retryCount++;
    if (retryCount > maxCount) {
      logger.info("Retry rule fixed: retried {} times - not retrying", maxCount);
      return false;
    }

    logger.info(
        "Retry rule fixed: starting retry {} of {} after sleep of {} seconds",
        retryCount,
        maxCount,
        intervalSeconds);

    TimeUnit.SECONDS.sleep(intervalSeconds);
    return true;
  }
}
