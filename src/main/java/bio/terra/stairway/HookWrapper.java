package bio.terra.stairway;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HookWrapper {
  private static final Logger logger = LoggerFactory.getLogger(HookWrapper.class);
  private final List<StairwayHook> stairwayHooks;

  private enum HookOperation {
    START_FLIGHT {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.startFlight(context));
      }

      void handleFlightHook(FlightContext context, FlightHook flightHook)
          throws InterruptedException {
        checkHookAction(flightHook.startFlight(context));
      }
    },
    START_STEP {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.startStep(context));
      }

      void handleStepHook(FlightContext context, StepHook stepHook) throws InterruptedException {
        checkHookAction(stepHook.startStep(context));
      }
    },
    END_STEP {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.endStep(context));
      }

      void handleStepHook(FlightContext context, StepHook stepHook) throws InterruptedException {
        checkHookAction(stepHook.endStep(context));
      }
    },
    END_FLIGHT {
      void handleHook(FlightContext context, StairwayHook stairwayHook)
          throws InterruptedException {
        checkHookAction(stairwayHook.endFlight(context));
      }

      void handleFlightHook(FlightContext context, FlightHook flightHook)
          throws InterruptedException {
        checkHookAction(flightHook.endFlight(context));
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

    void handleStepHook(FlightContext context, StepHook stepHook) throws InterruptedException {}

    void handleFlightHook(FlightContext context, FlightHook flightHook)
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
    List<FlightHook> flightHooks = new ArrayList<>();
    for (StairwayHook stairwayHook : stairwayHooks) {
      Optional<FlightHook> maybeFlightHook = stairwayHook.flightFactory(flightContext);
      maybeFlightHook.ifPresent(flightHooks::add);
    }
    // Then handle any flight hooks list from the factory
    handleFlightHookList(flightHooks, flightContext, HookOperation.START_FLIGHT);
    flightContext.setFlightHooks(flightHooks);
  }

  void startStep(FlightContext flightContext) throws InterruptedException {
    // First handle plain step hooks
    handleHookList(flightContext, HookOperation.START_STEP);
    // Use the factory to collect any step hooks for this step
    List<StepHook> stepHooks = new ArrayList<>();
    for (StairwayHook stairwayHook : stairwayHooks) {
      Optional<StepHook> maybeStepHook = stairwayHook.stepFactory(flightContext);
      maybeStepHook.ifPresent(stepHooks::add);
    }
    // Then handle any step hooks list from the factory
    handleStepHookList(stepHooks, flightContext, HookOperation.START_STEP);
    flightContext.setStepHooks(stepHooks);
  }

  void endStep(FlightContext flightContext) {
    // First handle plain step hooks
    handleHookList(flightContext, HookOperation.END_STEP);
    // Next handle the step hooks list from the flight context
    handleStepHookList(flightContext.getStepHooks(), flightContext, HookOperation.END_STEP);
    flightContext.setStepHooks(null);
  }

  void endFlight(FlightContext flightContext) {
    // First handle plain flight hooks
    handleHookList(flightContext, HookOperation.END_FLIGHT);
    // Next handle the flight hooks list from the flight context
    handleFlightHookList(flightContext.getFlightHooks(), flightContext, HookOperation.END_FLIGHT);
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

  private void handleStepHookList(
      List<StepHook> stepHooks, FlightContext context, HookOperation operation) {
    if (stepHooks != null) {
      for (StepHook stepHook : stepHooks) {
        try {
          operation.handleStepHook(context, stepHook);
        } catch (Exception ex) {
          logger.warn("Step Hook failed with exception", ex);
        }
      }
    }
  }

  private void handleFlightHookList(
      List<FlightHook> flightHooks, FlightContext context, HookOperation operation) {
    if (flightHooks != null) {
      for (FlightHook flightHook : flightHooks) {
        try {
          operation.handleFlightHook(context, flightHook);
        } catch (Exception ex) {
          logger.warn("Flight Hook failed with exception", ex);
        }
      }
    }
  }
}
