package bio.terra.stairway;

public class TestFlightYield extends Flight {

    public TestFlightYield(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        addStep(new TestStepYield());
        addStep(new TestStepResult());
    }

}
