package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class TestFlightWait extends Flight {

  public TestFlightWait(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepWait());
    addStep(new TestStepResult());
  }
}
