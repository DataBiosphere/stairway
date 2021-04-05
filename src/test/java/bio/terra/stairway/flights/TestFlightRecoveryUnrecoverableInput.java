package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

/** A {@link Flight} that tries to get input from a FlightMap that can't be recovered. */
public class TestFlightRecoveryUnrecoverableInput extends Flight {
  public static String INPUT_KEY = "bad input";

  public TestFlightRecoveryUnrecoverableInput(
      FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    // UUID can't be serialized as an input. This will throw on a deserialized FlightMap, but not on
    // FlightMap put/get.
    inputParameters.get(INPUT_KEY, UUID.class);
    // Step 1 - increment
    addStep(new TestStepIncrement());

    // Step 2 - stop - allow for failure
    addStep(new TestStepPause());

    // Step 3 - increment
    addStep(new TestStepIncrement());
  }
}
