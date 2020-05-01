package bio.terra.stairway;

public class TestStepStop implements Step {
    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        TestUtil.sleepStop();
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        TestUtil.sleepStop();
        return StepResult.getStepResultSuccess();
    }

}
