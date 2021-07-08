package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.RetryRuleFixedInterval;
import org.apache.commons.lang3.StringUtils;

public class TestFlightMultiStepRetry extends Flight {
  public TestFlightMultiStepRetry(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    RetryRule retryRule;

    // Pull out our parameters and feed them in to the step classes.
    String retryType = inputParameters.get("retryType", String.class);

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
    } else {
      throw new IllegalArgumentException("Invalid inputParameter retryType");
    } // Pull out our parameters and feed them in to the step classes.

    String filename = inputParameters.get("filename", String.class);
    String text = inputParameters.get("text", String.class);

    // Step 1 - test file existence
    addStep(new TestStepExistence(filename), retryRule);

    // Step 2 - create file
    addStep(new TestStepCreateFile(filename, text), retryRule);
  }
}
