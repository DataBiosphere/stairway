package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightMap;

public class TestFlightRerun extends Flight {

  public TestFlightRerun(
      FlightMap inputParameters, Object applicationContext, FlightDebugInfo debugInfo) {
    super(inputParameters, applicationContext, debugInfo);
    addStep(new TestStepForLoop());
    addStep(new TestStepResult());
  }
}
