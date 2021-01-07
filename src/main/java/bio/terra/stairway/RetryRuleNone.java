package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryRuleNone implements RetryRule {
  private static final Logger logger = LoggerFactory.getLogger(RetryRule.class);

  private static final RetryRuleNone retryRuleNoneSingleton = new RetryRuleNone();

  public static RetryRuleNone getRetryRuleNone() {
    return retryRuleNoneSingleton;
  }

  @Override
  public void initialize() {}

  @Override
  public boolean retrySleep() throws InterruptedException {
    logger.info("Retry rule none invoked - no retry");
    return false;
  }
}
