package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private Stairway stairway;

  public HookWrapper(Stairway stairway) {
    this.stairway = stairway;
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

  public HookAction startFlight(FlightContext context) {
    if (stairway.getStairwayHook() == null) {
      return HookAction.FAULT;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairway.getStairwayHook().startFlight(contextCopy);
    } catch (Exception ex) {
      logger.info("the hook did a bad thing");
      return HookAction.FAULT;
    }
  }

  public HookAction endFlight(FlightContext context) {
    if (stairway.getStairwayHook() == null) {
      return HookAction.FAULT;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairway.getStairwayHook().endFlight(contextCopy);
    } catch (Exception ex) {
      logger.info("the hook did a bad thing");
      return HookAction.FAULT;
    }
  }

  public HookAction startStep(FlightContext context) {
    if (stairway.getStairwayHook() == null) {
      return HookAction.FAULT;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairway.getStairwayHook().startStep(contextCopy);
    } catch (Exception ex) {
      logger.info("the hook did a bad thing");
      return HookAction.FAULT;
    }
  }

  public HookAction endStep(FlightContext context) {
    if (stairway.getStairwayHook() == null) {
      return HookAction.FAULT;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return stairway.getStairwayHook().endStep(contextCopy);
    } catch (Exception ex) {
      logger.info("the hook did a bad thing");
      return HookAction.FAULT;
    }
  }
}
