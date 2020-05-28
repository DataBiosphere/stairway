package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private StairwayHook stairwayHook;

  public HookWrapper(StairwayHook stairwayHook) {
    this.stairwayHook = stairwayHook;
  }

  public HookAction startFlight(FlightContext context) {
    if (stairwayHook == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairwayHook.startFlight(contextCopy);
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception: {}", ex);
    }
    return HookAction.CONTINUE;
  }

  public HookAction endFlight(FlightContext context) {
    if (stairwayHook == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairwayHook.endFlight(contextCopy);
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception: {}", ex);
    }
    return HookAction.CONTINUE;
  }

  public HookAction startStep(FlightContext context) {
    if (stairwayHook == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairwayHook.startStep(contextCopy);
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception: {}", ex);
    }
    return HookAction.CONTINUE;
  }

  public HookAction endStep(FlightContext context) {
    if (stairwayHook == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairwayHook.endStep(contextCopy);
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
}
