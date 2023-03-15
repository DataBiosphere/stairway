package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class TestFlightError extends Flight {

  public TestFlightError(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Undo will cause a dismal failure
    addStep(new TestStepErrorUndo());

    // Do will cause the initial failure
    addStep(new TestStepErrorDo());
  }
}
