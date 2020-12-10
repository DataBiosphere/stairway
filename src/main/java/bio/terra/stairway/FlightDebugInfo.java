package bio.terra.stairway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Debug information for a flight. Parameters here change how flights run to ensure debugability/
 * testability.
 */
public class FlightDebugInfo {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private boolean restartEachStep; // if true - restart the flight at each step

  // Use a builder so it is easy to add new fields
  public static class Builder {
    private boolean restartEachStep;

    public Builder restartEachStep(boolean restart) {
      this.restartEachStep = restart;
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
