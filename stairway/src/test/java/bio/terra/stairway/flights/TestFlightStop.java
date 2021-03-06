package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class TestFlightStop extends Flight {

  public TestFlightStop(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepIncrement());
    addStep(new TestStepStop());
    addStep(new TestStepIncrement());
  }
}
