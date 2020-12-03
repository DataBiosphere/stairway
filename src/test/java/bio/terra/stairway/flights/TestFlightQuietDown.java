package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightMap;

public class TestFlightQuietDown extends Flight {

  public TestFlightQuietDown(
      FlightMap inputParameters, Object applicationContext, FlightDebugInfo debugInfo) {
    super(inputParameters, applicationContext, debugInfo);
    addStep(new TestStepControlledSleep());
    addStep(new TestStepResult());
  }
}
