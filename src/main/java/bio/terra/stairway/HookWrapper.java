package bio.terra.stairway;

import java.util.ArrayList;
import java.util.Collections;
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

    void checkHookAction(HookAction action) {
      if (action != HookAction.CONTINUE) {
        logger.warn("Unexpected hook action: {}", action.name());
      }
    }
  }

  HookWrapper(List<StairwayHook> stairwayHooks) {
    this.stairwayHooks = stairwayHooks != null ? stairwayHooks : Collections.emptyList();
  }

  void startFlight(FlightContext flightContext) {
    handleHookList(flightContext, HookOperation.START_FLIGHT);
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
    handleHookList(flightContext, HookOperation.END_FLIGHT);
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
}
