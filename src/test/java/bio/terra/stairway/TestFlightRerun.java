package bio.terra.stairway;

public class TestFlightRerun extends Flight {

  public TestFlightRerun(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepForLoop());
    addStep(new TestStepResult());
  }
}
