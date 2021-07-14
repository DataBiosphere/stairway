package bio.terra.stairway.impl;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightSupport;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal flight support
 *
 * <p>This class provides additional members and methods for the {@link bio.terra.stairway.Flight}
 * implementation. It lets us better separate the internal implementation of flights and the
 * Stairway object rather than have them as public members referenced directly from the Flight
 * object.
 *
 * <p>As an implementation class, this code has access to the StairwayImpl and its package methods.
 */
public class FlightSupportImpl implements FlightSupport {
  private static final Logger logger = LoggerFactory.getLogger(FlightSupportImpl.class);

  // Reference to the internal Stairway implementation
  private final StairwayImpl stairwayImpl;

  // Debug state
  // These sets will only be populated if the corresponding debugInfo field is populated. If so,
  // each set keeps track of which steps have already been failed so we do not infinitely
  // retry.
  private final Set<Integer> debugStepsFailed;
  private final Set<String> debugDoStepsFailed;
  private final Set<String> debugUndoStepsFailed;

  FlightSupportImpl(StairwayImpl stairwayImpl) {
    this.stairwayImpl = stairwayImpl;
    debugStepsFailed = new HashSet<>();
    debugDoStepsFailed = new HashSet<>();
    debugUndoStepsFailed = new HashSet<>();
  }

  public void startFlightHook(FlightContext flightContext) throws InterruptedException {
    stairwayImpl.getHookWrapper().startFlight(flightContext);
  }

  public void endFlightHook(FlightContext flightContext) throws InterruptedException {
    stairwayImpl.getHookWrapper().endFlight(flightContext);
  }

  public void startStepHook(FlightContext flightContext) throws InterruptedException {
    stairwayImpl.getHookWrapper().startStep(flightContext);
  }

  public void endStepHook(FlightContext flightContext) throws InterruptedException {
    stairwayImpl.getHookWrapper().endStep(flightContext);
  }

  public boolean isQuietingDown() {
    return stairwayImpl.isQuietingDown();
  }

  public void exitFlight(FlightContext flightContext)
      throws InterruptedException, DatabaseOperationException, StairwayExecutionException, StairwayException {
    stairwayImpl.exitFlight(flightContext);
  }

  public void recordStep(FlightContext flightContext)
      throws StairwayException, InterruptedException {
    stairwayImpl.getFlightDao().step(flightContext);
  }

  public boolean getDebugRestartEachStep(FlightContext flightContext) {
    return (flightContext.getDebugInfo() != null
        && flightContext.getDebugInfo().getRestartEachStep());
  }

  /**
   * Helper function to replace the StepResult returned by a step based on {@link FlightDebugInfo}.
   *
   * @param initialResult the StepResult initially returned by the step.
   * @return StepResult to use for the step. May be the initial result.
   */
  public StepResult debugStatusReplacement(
      FlightContext flightContext, int stepsSize, StepResult initialResult) {
    FlightDebugInfo debugInfo = flightContext.getDebugInfo();
    if (debugInfo == null) {
      return initialResult;
    }

    // If we are in debug mode, in the DO and a failure is set for this step AND we have not already
    // failed here, then insert a failure. We do this right after the step completes but
    // before the flight logs it so that we can look for dangerous UNDOs.
    if (flightContext.isDoing()
        && debugInfo.getFailAtSteps() != null
        && debugInfo.getFailAtSteps().containsKey(flightContext.getStepIndex())
        && !debugStepsFailed.contains(flightContext.getStepIndex())) {
      StepStatus failStatus = debugInfo.getFailAtSteps().get(flightContext.getStepIndex());
      logger.info(
          "Failed for debug mode fail step at step {} with result {}",
          flightContext.getStepIndex(),
          failStatus);
      debugStepsFailed.add(flightContext.getStepIndex());
      return new StepResult(failStatus);
    }

    String currentStepClassName = flightContext.getStepClassName();

    // If we are in debug mode, doing, and a a failure is set for this step class name for do and we
    // have not already failed for this Step class, then insert a failure.
    if (flightContext.isDoing()
        && debugInfo.getDoStepFailures() != null
        && debugInfo.getDoStepFailures().containsKey(currentStepClassName)
        && !debugDoStepsFailed.contains(currentStepClassName)) {
      StepStatus failStatus = debugInfo.getDoStepFailures().get(currentStepClassName);
      logger.info(
          "Failed for debug mode fail do step at step {} with result {}",
          flightContext.getStepIndex(),
          failStatus);
      debugDoStepsFailed.add(currentStepClassName);
      return new StepResult(failStatus);
    }
    // If we are in debug mode, undoing, and a a failure is set for this step class name for undo
    // and we have not already failed for this Step class, then insert a failure.
    if (!flightContext.isDoing()
        && debugInfo.getUndoStepFailures() != null
        && debugInfo.getUndoStepFailures().containsKey(currentStepClassName)
        && !debugUndoStepsFailed.contains(currentStepClassName)) {
      StepStatus failStatus = debugInfo.getUndoStepFailures().get(currentStepClassName);
      logger.info(
          "Failed for debug mode fail do step at step {} with result {}",
          flightContext.getStepIndex(),
          failStatus);
      debugUndoStepsFailed.add(currentStepClassName);
      return new StepResult(failStatus);
    }
    // If we are in debug mode for failing at the last step, and this is the last step, insert a
    // failure.
    if (flightContext.isDoing()
        && debugInfo.getLastStepFailure()
        && flightContext.getStepIndex() == stepsSize - 1) {
      logger.info("Failed for debug mode last step failure.");
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
    }
    return initialResult;
  }
}
