package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class TestFlightRestarting extends Flight {

  public TestFlightRestarting(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    // Step 1 - increment
    addStep(new TestStepIncrement());

    // Step 2 - stop - allow for failure
    addStep(new TestStepIncrement());

    // Step 3 - increment
    addStep(new TestStepIncrement());
  }
}
