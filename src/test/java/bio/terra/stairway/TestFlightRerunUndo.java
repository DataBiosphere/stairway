package bio.terra.stairway;

public class TestFlightRerunUndo extends Flight {

    public TestFlightRerunUndo(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        addStep(new TestStepForLoopUndo());
        addStep(new TestStepResult());
    }

}
