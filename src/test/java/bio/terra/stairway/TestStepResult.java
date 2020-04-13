package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepResult implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap inputParameters = context.getInputParameters();
        String resultValue = inputParameters.get(MapKey.RESULT, String.class);

        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(MapKey.RESULT, resultValue);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}