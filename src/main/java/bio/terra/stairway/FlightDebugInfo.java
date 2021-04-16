package bio.terra.stairway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/**
 * Debug information for a flight. Parameters here change how flights run to ensure debugability/
 * testability.
 */
public class FlightDebugInfo {

  private static final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private boolean restartEachStep; // if true - restart the flight at each step
  // If true, make the flight's last do Step result in STEP_RESULT_FATAL_FAILURE after it executes.
  // This is useful for checking correct UNDO behavior for a whole flight.
  private boolean lastStepFailure;

  // Each entry in the map is the index at which we should insert a failure. Note that retryable
  // failures should only be inserted on steps that can be safely retried.
  private Map<Integer, StepStatus> failAtSteps;

  // Use a builder so it is easy to add new fields
  public static class Builder {
    private boolean restartEachStep;
    private boolean lastStepFailure;
    private Map<Integer, StepStatus> failAtSteps;

    public Builder restartEachStep(boolean restart) {
      this.restartEachStep = restart;
      return this;
    }

    public Builder lastStepFailure(boolean lastStepFailure) {
      this.lastStepFailure = lastStepFailure;
      return this;
    }

    public Builder failAtSteps(Map<Integer, StepStatus> failures) {
      this.failAtSteps = failures;
      return this;
    }

    /**
     * Construct a FlightDebugInfo instance based on the builder inputs
     *
     * @return FlightDebugInfo
     */
    public FlightDebugInfo build() {
      return new FlightDebugInfo(this);
    }
  }

  public static FlightDebugInfo.Builder newBuilder() {
    return new FlightDebugInfo.Builder();
  }

  public FlightDebugInfo(FlightDebugInfo.Builder builder) {
    this.restartEachStep = builder.restartEachStep;
    this.failAtSteps = builder.failAtSteps;
    this.lastStepFailure = builder.lastStepFailure;
  }

  public FlightDebugInfo() {
    this.restartEachStep = false;
  }

  public boolean getRestartEachStep() {
    return this.restartEachStep;
  }

  public void setRestartEachStep(boolean restart) {
    this.restartEachStep = restart;
  }

  public boolean getLastStepFailure() {
    return lastStepFailure;
  }

  public void setLastStepFailure(boolean lastStepFailure) {
    this.lastStepFailure = lastStepFailure;
  }

  public Map<Integer, StepStatus> getFailAtSteps() {
    return this.failAtSteps;
  }

  public void setFailAtSteps(Map<Integer, StepStatus> failures) {
    this.failAtSteps = failures;
  }

  static ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  @Override
  public String toString() {
    // Is it better to store the ObjectMapper for some reason?
    try {
      return getObjectMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
    }
  }
}
