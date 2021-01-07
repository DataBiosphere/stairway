package bio.terra.stairway;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryRuleExponentialBackoff implements RetryRule {
  private static final Logger logger = LoggerFactory.getLogger(RetryRule.class);

  private final long initialIntervalSeconds;
  private final long maxIntervalSeconds;
  private final long maxOperationTimeSeconds;

  private LocalDateTime endTime;
  private long intervalSeconds;

  /**
   * Retry with exponential backoff
   *
   * @param initialIntervalSeconds - starting interval; will double up to max interval
   * @param maxIntervalSeconds - maximum interval to ever sleep
   * @param maxOperationTimeSeconds - maximum amount of time to allow for the operation
   */
  public RetryRuleExponentialBackoff(
      long initialIntervalSeconds, long maxIntervalSeconds, long maxOperationTimeSeconds) {
    this.initialIntervalSeconds = initialIntervalSeconds;
    this.maxIntervalSeconds = maxIntervalSeconds;
    this.maxOperationTimeSeconds = maxOperationTimeSeconds;
  }

  @Override
  public void initialize() {
    intervalSeconds = initialIntervalSeconds;
    endTime = LocalDateTime.now().plus(Duration.ofSeconds(maxOperationTimeSeconds));
  }

  @Override
  public boolean retrySleep() throws InterruptedException {
    LocalDateTime now = LocalDateTime.now();
    if (now.isAfter(endTime)) {
      logger.info(
          "Retry rule exponential: now ({}) is past max time {} - not retrying", now, endTime);
      return false;
    }

    logger.info(
        "Retry rule exponential: now ({}) is before {} - retrying after sleep of {} seconds",
        now,
        endTime,
        intervalSeconds);

    TimeUnit.SECONDS.sleep(intervalSeconds);
    intervalSeconds = intervalSeconds + intervalSeconds;
    if (intervalSeconds > maxIntervalSeconds) {
      intervalSeconds = maxIntervalSeconds;
    }
    return true;
  }
}
