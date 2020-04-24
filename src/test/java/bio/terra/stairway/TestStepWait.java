package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepWait implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        return new StepResult(StepStatus.STEP_RESULT_WAIT);
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
