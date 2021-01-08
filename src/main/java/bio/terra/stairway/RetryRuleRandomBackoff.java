package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RetryRuleRandomBackoff implements RetryRule {
  private static final Logger logger = LoggerFactory.getLogger(RetryRule.class);

  private final long operationIncrementMilliseconds;
  private final int maxConcurrency;
  private final int maxCount;

  // Initialized parameters
  private int retryCount;

  /**
   * Retry with random backoff for concurrent threads. Assume operation requires
   * operationIncrementMilliseconds. We want to spread maxConcurrency threads so that we will do
   * roughly one at a time. So we get a random integer between 0 and maxConcurrency-1 and multiply
   * that by the operation time. We sleep that long and retry.
   *
   * @param operationIncrementMilliseconds - interval to spread across concurrency
   * @param maxConcurrency - maximum concurrent threads to back off
   * @param maxCount - maximum times to retry
   */
  public RetryRuleRandomBackoff(
      long operationIncrementMilliseconds, int maxConcurrency, int maxCount) {
    this.operationIncrementMilliseconds = operationIncrementMilliseconds;
    this.maxConcurrency = maxConcurrency;
    this.maxCount = maxCount;
  }

  @Override
  public void initialize() {
    retryCount = 0;
  }

  @Override
  public boolean retrySleep() throws InterruptedException {
    retryCount++;
    if (retryCount > maxCount) {
      logger.info("Retry rule random: retried {} times - not retrying", maxCount);
      return false;
    }

    int sleepUnits = ThreadLocalRandom.current().nextInt(0, maxConcurrency);
    long ms = sleepUnits * operationIncrementMilliseconds;
    logger.info(
        "Retry rule random: starting retry {} of {} after sleep of {} milliseconds",
        retryCount,
        maxCount,
        ms);

    TimeUnit.MILLISECONDS.sleep(ms);
    return true;
  }
}
