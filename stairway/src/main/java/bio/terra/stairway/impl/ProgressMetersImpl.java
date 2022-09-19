package bio.terra.stairway.impl;

import bio.terra.stairway.ProgressMeter;
import bio.terra.stairway.ProgressMeterReader;
import bio.terra.stairway.exception.InvalidMeterName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class ProgressMetersImpl implements ProgressMeterReader {
  /** FlightMap key for persistedStateMap for storing the progress meters */
  public static final String STAIRWAY_PROGRESS_METERS = "StairwayProgressMeters";

  /** Stake out a chunk of the meter name space for stairway meters */
  public static final String STAIRWAY_RESERVED_METER_PREFIX = "STAIRWAY";

  /** Progress meter for the flight step we are on */
  public static final String STAIRWAY_STEP_PROGRESS =
      STAIRWAY_RESERVED_METER_PREFIX + "_STEP_PROGRESS";

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
  @VisibleForTesting
  public ProgressMetersImpl(PersistedStateMap persistedStateMap) {
    this.persistedStateMap = persistedStateMap;
  }

  void setProgressMeter(String name, long v1, long v2) throws InterruptedException {
    if (StringUtils.startsWith(name, STAIRWAY_RESERVED_METER_PREFIX)) {
      throw new InvalidMeterName(
          "Invalid meter name: it cannot start with " + STAIRWAY_RESERVED_METER_PREFIX);
    }
    setProgressMeterWorker(name, v1, v2);
  }

  void setStairwayStepProgress(long v1, long v2) throws InterruptedException {
    setProgressMeterWorker(STAIRWAY_STEP_PROGRESS, v1, v2);
  }

  void setProgressMeterWorker(String name, long v1, long v2) throws InterruptedException {
    Map<String, ProgressMeter> meters = getMetersFromMap();
    meters.put(name, new ProgressMeter(v1, v2));
    putMetersToMap(meters);
    persistedStateMap.flush();
  }

  @Override
  public ProgressMeter getProgressMeter(String name) {
    Map<String, ProgressMeter> meters = getMetersFromMap();
    return meters.get(name);
  }

  @Override
  public ProgressMeter getFlightStepProgressMeter() {
    return getProgressMeter(STAIRWAY_STEP_PROGRESS);
  }

  private Map<String, ProgressMeter> getMetersFromMap() {
    Map<String, ProgressMeter> meters =
        persistedStateMap.get(STAIRWAY_PROGRESS_METERS, new TypeReference<>() {});
    return Optional.ofNullable(meters).orElseGet(HashMap::new);
  }

  private void putMetersToMap(Map<String, ProgressMeter> meters) {
    persistedStateMap.put(STAIRWAY_PROGRESS_METERS, meters);
  }
}
