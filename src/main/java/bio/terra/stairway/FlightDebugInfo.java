package bio.terra.stairway;

public class FlightDebugInfo {
  private boolean restartEachStep; // true - restart the flight at each step

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

  public boolean getRestartEachStep() {
    return this.restartEachStep;
  }

  public void setRestartEachStep(boolean restart) {
    this.restartEachStep = restart;
  }
}
