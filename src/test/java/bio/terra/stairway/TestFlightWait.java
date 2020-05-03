package bio.terra.stairway;

public class TestFlightWait extends Flight {

  public TestFlightWait(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepWait());
    addStep(new TestStepResult());
  }
}
