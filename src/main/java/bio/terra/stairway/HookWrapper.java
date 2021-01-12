package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private final List<StairwayHook> stairwayHooks;

  private enum HookOperation {
    START_FLIGHT,
    START_STEP,
    END_STEP,
    END_FLIGHT,
    STATE_TRANSITION
  }

  public HookWrapper(List<StairwayHook> stairwayHooks) {
    this.stairwayHooks = stairwayHooks;
  }

  public void startFlight(FlightContext flightContext) {
    handleHookList(flightContext, HookOperation.START_FLIGHT);
  }

  public void startStep(FlightContext flightContext) throws InterruptedException {
    if (stairwayHooks != null) {
      FlightContext contextCopy = makeCopy(flightContext);
      // First handle plain step hooks
      handleHookList(contextCopy, HookOperation.START_STEP);
      // Use the factory to collect any step hooks for this step
      List<StepHook> stepHooks = new ArrayList<>();
      for (StairwayHook stairwayHook : stairwayHooks) {
        Optional<StepHook> maybeStepHook = stairwayHook.stepFactory(contextCopy);
        maybeStepHook.ifPresent(stepHooks::add);
      }
      // Then handle an step hooks list from the factory
      handleStepHookList(stepHooks, contextCopy, HookOperation.START_STEP);
      flightContext.setStepHooks(stepHooks);
    }
  }

  public void endStep(FlightContext flightContext) {
    if (stairwayHooks != null) {
      FlightContext contextCopy = makeCopy(flightContext);
      // First handle plain step hooks
      handleHookList(contextCopy, HookOperation.END_STEP);
      // Next handle the step hooks list from the flight context
      handleStepHookList(flightContext.getStepHooks(), contextCopy, HookOperation.END_STEP);
      flightContext.setStepHooks(null);
    }
  }

  public void endFlight(FlightContext flightContext) {
    handleHookList(flightContext, HookOperation.END_FLIGHT);
  }

  private interface HookInterface {
    void hook(FlightContext context) throws InterruptedException;
  }

  private void handleHookList(FlightContext context, HookOperation operation) {
    if (stairwayHooks != null) {
      for (StairwayHook stairwayHook : stairwayHooks) {
        switch (operation) {
          case START_FLIGHT:
            handleHook(makeCopy(context), stairwayHook::startFlight);
            break;

          case START_STEP:
            // copy is made in the caller
            handleHook(context, stairwayHook::startStep);
            break;

          case END_STEP:
            // copy is made in the caller
            handleHook(context, stairwayHook::endStep);
            break;

          case END_FLIGHT:
            handleHook(makeCopy(context), stairwayHook::endFlight);
            break;

          case STATE_TRANSITION:
            handleHook(makeCopy(context), stairwayHook::stateTransition);
            break;
        }
      }
    }
  }

  private void handleStepHookList(List<StepHook> stepHooks, FlightContext context, HookOperation operation) {
    if (stepHooks != null) {
      for (StepHook stepHook : stepHooks) {
        switch (operation) {
          case START_STEP:
            handleHook(context, stepHook::startStep);
            break;

          case END_STEP:
            handleHook(context, stepHook::endStep);
            break;

          case END_FLIGHT:
          case START_FLIGHT:
          case STATE_TRANSITION:
            break;
        }
      }
    }
  }

  private void handleHook(FlightContext context, HookInterface hookMethod) {
    try {
      hookMethod.hook(context);
    } catch (Exception ex) {
      logger.info("Stairway Hook failed with exception", ex);
    }
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
    fc_new.setDebugInfo(fc.getDebugInfo());
    return fc_new;
  }
}
