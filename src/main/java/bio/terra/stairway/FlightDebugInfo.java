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

  // Map from Step class name, e.g. MyStep.class.getName(), to the failure to insert on do. Note
  // that retryable failures should only be inserted on step's with do operations that can be safely
  // retried.
  private Map<String, StepStatus> doStepFailures;

  // Map from Step class name, e.g. MyStep.class.getName(), to the failure to insert on undo. Note
  // that retryable failures should only be inserted on step's with undo operations that can be
  // safely retried.
  private Map<String, StepStatus> undoStepFailures;

  // Use a builder so it is easy to add new fields
  public static class Builder {
    private boolean restartEachStep;
    private boolean lastStepFailure;
    private Map<Integer, StepStatus> failAtSteps;
    private Map<String, StepStatus> doStepFailures;
    private Map<String, StepStatus> undoStepFailures;

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

    public Builder doStepFailures(Map<String, StepStatus> doStepFailures) {
      this.doStepFailures = doStepFailures;
      return this;
    }

    public Builder undoStepFailures(Map<String, StepStatus> undoStepFailures) {
      this.undoStepFailures = undoStepFailures;
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

  public FlightDebugInfo(Builder builder) {
    this.restartEachStep = builder.restartEachStep;
    this.failAtSteps = builder.failAtSteps;
    this.lastStepFailure = builder.lastStepFailure;
    this.doStepFailures = builder.doStepFailures;
    this.undoStepFailures = builder.undoStepFailures;
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

  public Map<String, StepStatus> getDoStepFailures() {
    return doStepFailures;
  }

  public void setDoStepFailures(Map<String, StepStatus> doStepFailures) {
    this.doStepFailures = doStepFailures;
  }

  public Map<String, StepStatus> getUndoStepFailures() {
    return undoStepFailures;
  }

  public void setUndoStepFailures(Map<String, StepStatus> undoStepFailures) {
    this.undoStepFailures = undoStepFailures;
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
