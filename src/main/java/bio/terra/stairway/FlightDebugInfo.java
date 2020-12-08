package bio.terra.stairway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/**
 * Debug information for a flight. Parameters here change how flights run to ensure debugability/
 * testability.
 */
public class FlightDebugInfo {

  private static ObjectMapper objectMapper;
  private boolean restartEachStep; // if true - restart the flight at each step
  private Map<Integer, StepStatus> failAtSteps; // For each entry, the flight will return a given
  // StepStatus for the step at the given position.

  // Use a builder so it is easy to add new fields
  public static class Builder {
    private boolean restartEachStep;
    private Map<Integer, StepStatus> failAtSteps;

    public Builder restartEachStep(boolean restart) {
      this.restartEachStep = restart;
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

  public Map<Integer, StepStatus> getFailAtSteps() {
    return this.failAtSteps;
  }

  public void setFailAtSteps(Map<Integer, StepStatus> failures) {
    this.failAtSteps = failures;
  }

  static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper();
    }
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
