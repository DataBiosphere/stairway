package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

// Do nothing flight to test enumeration and filtering
public class TestFlightEnum1 extends Flight {
  public TestFlightEnum1(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
  }
}
