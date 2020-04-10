package bio.terra.stairway;

public class TestFlightQuietDown extends Flight {

    public TestFlightQuietDown(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        addStep(new TestStepSleep());
        addStep(new TestStepResult());
    }

}
