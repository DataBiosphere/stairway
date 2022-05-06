package bio.terra.stairway.impl;

import bio.terra.stairway.FlightMap;
import bio.terra.stairway.ProgressMeterData;
import bio.terra.stairway.ProgressMeterReader;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProgressMetersImpl implements ProgressMeterReader {
  /** FlightMap key for persistedStateMap for storing the progress meters */
  public static final String STAIRWAY_PROGRESS_METERS = "StairwayProgressMeters";

  /** Progress meter for the flight step we are on */
  public static final String STAIRWAY_STEP_PROGRESS = "StairwayStepProgress";

  // Progress meters
  private final PersistedStateMap persistedStateMap;

  /**
   * Constructor for new flight context creation: create the map
   *
   * @param flightDao Address of the DAO object for writing the map
   * @param flightId Flight id for this flight
   */
  ProgressMetersImpl(FlightDao flightDao, String flightId) {
    this.persistedStateMap = new PersistedStateMap(flightDao, flightId);
  }

  /**
   * Constructor for DAO flight context creation
   *
   * @param persistedStateMap flightMap for persisted data
   */
  ProgressMetersImpl(FlightMap persistedStateMap) {
    this.persistedStateMap = (PersistedStateMap) persistedStateMap;
  }

  void setProgressMeter(String name, long v1, long v2) throws InterruptedException {
    Map<String, ProgressMeterData> meters = getMetersFromMap();
    meters.put(name, new ProgressMeterData(v1, v2));
    putMetersToMap(meters);
    persistedStateMap.flush();
  }

  @Override
  public Optional<ProgressMeterData> getProgressMeter(String name) {
    Map<String, ProgressMeterData> meters = getMetersFromMap();
    return Optional.ofNullable(meters.get(name));
  }

  @Override
  public Optional<ProgressMeterData> getFlightStepProgressMeter() {
    return getProgressMeter(STAIRWAY_STEP_PROGRESS);
  }

  private Map<String, ProgressMeterData> getMetersFromMap() {
    Map<String, ProgressMeterData> meters =
        persistedStateMap.get(STAIRWAY_PROGRESS_METERS, new TypeReference<>() {});
    return Optional.ofNullable(meters).orElseGet(HashMap::new);
  }

  private void putMetersToMap(Map<String, ProgressMeterData> meters) {
    persistedStateMap.put(STAIRWAY_PROGRESS_METERS, meters);
  }
}
