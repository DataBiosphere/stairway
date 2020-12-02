package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private StairwayHook stairwayHook;

  public HookWrapper(StairwayHook stairwayHook) {
    this.stairwayHook = stairwayHook;
  }

  public HookAction startFlight(FlightContext flightContext) {
    return handleFlightHook(flightContext, context -> stairwayHook.startFlight(context));
  }

  public HookAction startStep(FlightContext flightContext) {
    return handleStepHook(flightContext, context -> stairwayHook.newStepHook().startStep(context));
  }

  public HookAction endStep(FlightContext flightContext) {
    return handleStepHook(flightContext, context -> stairwayHook.newStepHook().endStep(context));
  }

  public HookAction endFlight(FlightContext flightContext) {
    return handleFlightHook(flightContext, context -> stairwayHook.endFlight(context));
  }

  private interface HookInterface {
    HookAction hook(FlightContext context) throws InterruptedException;
  }

  private HookAction handleStepHook(FlightContext context, HookInterface hookMethod) {
    if (stairwayHook == null || stairwayHook.newStepHook() == null) {
      return HookAction.CONTINUE;
    }
    return handleHook(context, hookMethod);
  }

  private HookAction handleFlightHook(FlightContext context, HookInterface hookMethod) {
    if (stairwayHook == null ) {
      return HookAction.CONTINUE;
    }
    return handleHook(context, hookMethod);
  }

  private HookAction handleHook(FlightContext context, HookInterface hookMethod) {
    FlightContext contextCopy = makeCopy(context);
    try {
      return hookMethod.hook(contextCopy);
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception", ex);
    }
    return HookAction.CONTINUE;
  }

  private FlightContext makeCopy(FlightContext fc) {
    FlightContext fc_new =
        new FlightContext(fc.getInputParameters(), fc.getFlightClassName(), fc.getStepClassNames());
    fc_new.setDirection(fc.getDirection());
    fc_new.setFlightId(fc.getFlightId());
    fc_new.setFlightStatus(fc.getFlightStatus());
    fc_new.setStairway(fc.getStairway());
    fc_new.setStepIndex(fc.getStepIndex());
    fc_new.setResult(fc.getResult());
    fc_new.setRerun(fc.isRerun());
    return fc_new;
  }
}
