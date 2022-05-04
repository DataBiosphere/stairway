package bio.terra.stairway.impl;

import bio.terra.stairway.FlightMap;
import bio.terra.stairway.ProgressMeterData;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public class ProgressUtils {
  private ProgressUtils() {}

  /** FlightMap key for persistedStateMap for storing the progress meters */
  public static final String STAIRWAY_PROGRESS_METERS = "StairwayProgressMeters";

  /** Progress meter for the flight step we are on */
  public static final String STAIRWAY_STEP_PROGRESS = "StairwayStepProgress";

  static List<ProgressMeterData> getProgressMetersFromMap(FlightMap persistedStateMap) {
    List<ProgressMeterData> progressData =
        persistedStateMap.get(STAIRWAY_PROGRESS_METERS, List.class);
    return Optional.ofNullable(progressData).orElseGet(ArrayList::new);
  }

  static void setProgressMeter(FlightMap persistedStateMap, String name, long v1, long v2) {
    List<ProgressMeterData> progressData = getProgressMetersFromMap(persistedStateMap);
    ProgressMeterData meterData = new ProgressMeterData(name, v1, v2);

    // Search the list - we expect the list to be very short, so not worth allocating
    // a complex lookup structure.
    int index =
        IntStream.range(0, progressData.size())
            .filter(i -> progressData.get(i).getName().equals(name))
            .findFirst()
            .orElse(-1);
    if (index == -1) {
      progressData.add(meterData);
    } else {
      progressData.set(index, meterData);
    }

    persistedStateMap.put(STAIRWAY_PROGRESS_METERS, progressData);
  }

}
