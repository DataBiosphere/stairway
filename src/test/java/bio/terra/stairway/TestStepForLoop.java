package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TestStepForLoop implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap inputParameters = context.getInputParameters();
        int startCounter = inputParameters.get(MapKey.COUNTER_START, Integer.class);
        int endCounter = inputParameters.get(MapKey.COUNTER_END, Integer.class);
        Integer stopCounter = inputParameters.get(MapKey.COUNTER_STOP, Integer.class);

        FlightMap workingMap = context.getWorkingMap();
        Integer currentCounter = workingMap.get(MapKey.COUNTER, Integer.class);
        if (currentCounter == null) {
            logger.debug("Start of for loop");
            currentCounter = startCounter;
        }

        // Do processing for current counter
        logger.debug("For " + startCounter + " to " + endCounter + " processing at " + currentCounter);

        currentCounter++;
        workingMap.put(MapKey.COUNTER, currentCounter);

        if (currentCounter >= endCounter) {
            logger.debug("End of for loop");
            return StepResult.getStepResultSuccess();
        }

        if (TestStopController.getControl() == 0 && stopCounter != null && stopCounter.equals(currentCounter)) {
            logger.debug("TestStepForLoop going to sleep");
            TimeUnit.HOURS.sleep(1);
        } else {
            logger.debug("TestStepForLoop keep going");
        }

        return new StepResult(StepStatus.STEP_RESULT_RERUN);
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
