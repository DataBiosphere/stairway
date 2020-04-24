package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TestStepSleep implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        logger.info("SleepStep - Flight: " + context.getFlightId() +
                "; stairway: " + context.getStairway().getStairwayName());

        FlightMap inputParameters = context.getInputParameters();
        int sleepSeconds = inputParameters.get(MapKey.SLEEP_SECONDS, Integer.class);
        TimeUnit.SECONDS.sleep(sleepSeconds);

        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(MapKey.RESULT, "sleep step woke up");
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
