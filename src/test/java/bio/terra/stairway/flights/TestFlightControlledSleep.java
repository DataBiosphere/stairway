package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class TestFlightControlledSleep extends Flight {

  public TestFlightControlledSleep(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepControlledSleep());
  }
}
