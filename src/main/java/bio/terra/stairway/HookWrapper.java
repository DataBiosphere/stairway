package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private Stairway stairway;

  public HookWrapper(Stairway stairway) {
    this.stairway = stairway;
  }

  public HookAction startFlight(FlightContext context) {
    return handleHook(context, HookType.startFlight);
  }

  public HookAction endFlight(FlightContext context) {
    return handleHook(context, HookType.endFlight);
  }

  public HookAction startStep(FlightContext context) {
    return handleHook(context, HookType.startStep);
  }

  public HookAction endStep(FlightContext context) {
    return handleHook(context, HookType.endStep);
  }

  private HookAction handleHook(FlightContext context, HookType type) {
    if (stairway.getStairwayHook() == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      switch (type) {
        case startFlight:
          return stairway.getStairwayHook().startFlight(contextCopy);
        case endFlight:
          return stairway.getStairwayHook().endFlight(contextCopy);
        case startStep:
          return stairway.getStairwayHook().startStep(contextCopy);
        case endStep:
          return stairway.getStairwayHook().endStep(contextCopy);
      }
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception: {}", ex);
    }
    return HookAction.CONTINUE;
  }

  private FlightContext makeCopy(FlightContext fc) {
    FlightContext fc_new = new FlightContext(fc.getInputParameters(), fc.getFlightClassName());
    fc_new.setDirection(fc.getDirection());
    fc_new.setFlightId(fc.getFlightId());
    fc_new.setFlightStatus(fc.getFlightStatus());
    fc_new.setStairway(fc.getStairway());
    fc_new.setStepIndex(fc.getStepIndex());
    fc_new.setResult(fc.getResult());
    fc_new.setRerun(fc.isRerun());
    return fc_new;
  }

  private enum HookType {
    startFlight,
    endFlight,
    startStep,
    endStep
  }
}
