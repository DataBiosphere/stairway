package bio.terra.stairway;

import java.util.Optional;

public interface ProgressMeterReader {
  /**
   * Retrieve a progress meter by name
   *
   * @param name name to look up
   * @return progress meter data
   */
  Optional<ProgressMeterData> getProgressMeter(String name);

  /**
   * Retrieve the progress meter for the flight steps. The returned v1 is the step in progress. The
   * returned v2 is the total number of steps. Note that the steps are 0-based.
   *
   * @return meter data for flight steps
   */
  Optional<ProgressMeterData> getFlightStepProgressMeter();
}
