package bio.terra.stairway;

import org.apache.commons.lang3.time.StopWatch;

/**
 * Example of a hook to go in client code that allows arbitrary state to be kept between start & end
 * step.
 */
public class MyStopWatchStairwayHook implements StairwayHookV2 {
  String serviceState = "foo";

  @Override
  public StepHook newStep(FlightContext context) {
    return new MyStepHook();
  }

  @Override
  public FlightHook newFlight(FlightContext context) {
    return null;
  }

  public class MyStepHook implements StairwayHookV2.StepHook {
    private String stepState;
    StopWatch stopWatch;

    @Override
    public void start(FlightContext context) {
      stepState = "hello" + context.getStepIndex() + serviceState;
      stopWatch = StopWatch.createStarted();
    }

    @Override
    public void end(FlightContext context, StepResult stepResult) {
      stopWatch.stop();
      // do something with the stopwatch result.
    }
  }
}
