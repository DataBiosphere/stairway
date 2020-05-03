package bio.terra.stairway;

public class TestFlightQuietDown extends Flight {

  public TestFlightQuietDown(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepControlledSleep());
    addStep(new TestStepResult());
  }
}
