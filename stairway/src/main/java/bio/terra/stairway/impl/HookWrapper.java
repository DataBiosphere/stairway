package bio.terra.stairway.impl;

import bio.terra.stairway.DynamicHook;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StairwayHook;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private final List<StairwayHook> stairwayHooks;

  private enum HookOperation {
    START_FLIGHT {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.startFlight(context));
      }

      void handleDynamicHook(FlightContext context, DynamicHook flightHook)
          throws InterruptedException {
        checkHookAction(flightHook.start(context));
      }
    },
    START_STEP {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.startStep(context));
      }

      void handleDynamicHook(FlightContext context, DynamicHook stepHook)
          throws InterruptedException {
        checkHookAction(stepHook.start(context));
      }
    },
    END_STEP {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.endStep(context));
      }

      void handleDynamicHook(FlightContext context, DynamicHook stepHook)
          throws InterruptedException {
        checkHookAction(stepHook.end(context));
      }
    },
    END_FLIGHT {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.endFlight(context));
      }

      void handleDynamicHook(FlightContext context, DynamicHook flightHook)
          throws InterruptedException {
        checkHookAction(flightHook.end(context));
      }
    },
    STATE_TRANSITION {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.stateTransition(context));
      }
    };

    abstract void handleHook(FlightContext context, StairwayHook stairwayHook)
        throws InterruptedException;

    void handleDynamicHook(FlightContext context, DynamicHook stepHook)
        throws InterruptedException {}

    void checkHookAction(HookAction action) {
      if (action != HookAction.CONTINUE) {
        logger.warn("Unexpected hook action: {}", action.name());
      }
    }
  }

  HookWrapper(List<StairwayHook> stairwayHooks) {
    this.stairwayHooks = stairwayHooks;
  }

  void startFlight(FlightContext flightContext) throws InterruptedException {
    // First handle plain flight hooks
    handleHookList(flightContext, HookOperation.START_FLIGHT);
    // Use the factory to collect any flight hooks for this flight
    List<DynamicHook> flightHooks = new ArrayList<>();
    for (StairwayHook stairwayHook : stairwayHooks) {
      Optional<DynamicHook> maybeFlightHook = stairwayHook.flightFactory(flightContext);
      maybeFlightHook.ifPresent(flightHooks::add);
    }
    // Then handle any flight hooks list from the factory
    handleDynamicHookList(flightHooks, flightContext, HookOperation.START_FLIGHT);
    flightContext.setFlightHooks(flightHooks);
  }

  void startStep(FlightContext flightContext) throws InterruptedException {
    // First handle plain step hooks
    handleHookList(flightContext, HookOperation.START_STEP);
    // Use the factory to collect any step hooks for this step
    List<DynamicHook> stepHooks = new ArrayList<>();
    for (StairwayHook stairwayHook : stairwayHooks) {
      Optional<DynamicHook> maybeStepHook = stairwayHook.stepFactory(flightContext);
      maybeStepHook.ifPresent(stepHooks::add);
    }
    // Then handle any step hooks list from the factory
    handleDynamicHookList(stepHooks, flightContext, HookOperation.START_STEP);
    flightContext.setStepHooks(stepHooks);
  }

  void endStep(FlightContext flightContext) {
    // First handle plain step hooks
    handleHookList(flightContext, HookOperation.END_STEP);
    // Next handle the step hooks list from the flight context
    handleDynamicHookList(flightContext.getStepHooks(), flightContext, HookOperation.END_STEP);
    flightContext.setStepHooks(null);
  }

  void endFlight(FlightContext flightContext) {
    // First handle plain flight hooks
    handleHookList(flightContext, HookOperation.END_FLIGHT);
    // Next handle the flight hooks list from the flight context
    handleDynamicHookList(flightContext.getFlightHooks(), flightContext, HookOperation.END_FLIGHT);
    flightContext.setFlightHooks(null);
  }

  void stateTransition(FlightContext flightContext) {
    handleHookList(flightContext, HookOperation.STATE_TRANSITION);
  }

  private void handleHookList(FlightContext context, HookOperation operation) {
    for (StairwayHook stairwayHook : stairwayHooks) {
      try {
        operation.handleHook(context, stairwayHook);
      } catch (Exception ex) {
        logger.warn("Stairway Hook failed with exception", ex);
      }
    }
  }

  private void handleDynamicHookList(
      List<DynamicHook> dynamicHooks, FlightContext context, HookOperation operation) {
    if (dynamicHooks != null) {
      for (DynamicHook dynamicHook : dynamicHooks) {
        try {
          operation.handleDynamicHook(context, dynamicHook);
        } catch (Exception ex) {
          logger.warn("Dynamic Hook failed with exception", ex);
        }
      }
    }
  }
}
