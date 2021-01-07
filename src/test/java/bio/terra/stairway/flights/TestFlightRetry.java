package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.RetryRuleFixedInterval;
import bio.terra.stairway.RetryRuleRandomBackoff;
import org.apache.commons.lang3.StringUtils;

public class TestFlightRetry extends Flight {

  public TestFlightRetry(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    RetryRule retryRule;

    // Pull out our parameters and feed them in to the step classes.
    String retryType = inputParameters.get("retryType", String.class);
    Integer failCount = inputParameters.get("failCount", Integer.class);

    if (StringUtils.equals("fixed", retryType)) {
      Integer intervalSeconds = inputParameters.get("intervalSeconds", Integer.class);
      Integer maxCount = inputParameters.get("maxCount", Integer.class);
      retryRule = new RetryRuleFixedInterval(intervalSeconds, maxCount);
    } else if (StringUtils.equals("exponential", retryType)) {
      Long initialIntervalSeconds = inputParameters.get("initialIntervalSeconds", Long.class);
      Long maxIntervalSeconds = inputParameters.get("maxIntervalSeconds", Long.class);
      Long maxOperationTimeSeconds = inputParameters.get("maxOperationTimeSeconds", Long.class);
      retryRule =
          new RetryRuleExponentialBackoff(
              initialIntervalSeconds, maxIntervalSeconds, maxOperationTimeSeconds);
    } else if (StringUtils.equals("random", retryType)) {
      long operationMilliseconds = inputParameters.get("operationMilliseconds", Long.class);
      int maxConcurrency = inputParameters.get("maxConcurrency", Integer.class);
      int maxCount = inputParameters.get("maxCount", Integer.class);
      retryRule = new RetryRuleRandomBackoff(operationMilliseconds, maxConcurrency, maxCount);
    } else {
      throw new IllegalArgumentException("Invalid inputParameter retryType");
    }

    addStep(new TestStepRetry(failCount), retryRule);
  }
}
