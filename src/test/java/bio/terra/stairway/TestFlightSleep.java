package bio.terra.stairway;

public class TestFlightSleep extends Flight {

  public TestFlightSleep(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    addStep(new TestStepSleep());
    addStep(new TestStepResult());
  }
}
