package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private StairwayHook stairwayHook;

  public HookWrapper(StairwayHook stairwayHook) {
    this.stairwayHook = stairwayHook;
  }

  private HookAction handleHook(FlightContext context, HookInterface hookMethod) {
    if (stairwayHook == null) {
      return HookAction.CONTINUE;
    }
    FlightContext contextCopy = makeCopy(context);
    try {
      return hookMethod.hook(contextCopy);
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

  // Hooks
  public HookAction startFlight(FlightContext context) {
    return handleHook(context, new StartFlightCommand());
  }

  public HookAction startStep(FlightContext context) {
    return handleHook(context, new StartStepCommand());
  }

  public HookAction endStep(FlightContext context) {
    return handleHook(context, new EndStepCommand());
  }

  public HookAction endFlight(FlightContext context) {
    return handleHook(context, new EndFlightCommand());
  }

  // Specify hook action
  private interface HookInterface {
    HookAction hook(FlightContext context) throws InterruptedException;
  }

  private class StartFlightCommand implements HookInterface {
    public HookAction hook(FlightContext context) throws InterruptedException {
      return stairwayHook.startFlight(context);
    }
  }

  private class StartStepCommand implements HookInterface {
    public HookAction hook(FlightContext context) throws InterruptedException {
      return stairwayHook.startStep(context);
    }
  }

  private class EndStepCommand implements HookInterface {
    public HookAction hook(FlightContext context) throws InterruptedException {
      return stairwayHook.endStep(context);
    }
  }

  private class EndFlightCommand implements HookInterface {
    public HookAction hook(FlightContext context) throws InterruptedException {
      return stairwayHook.endFlight(context);
    }
  }
}
