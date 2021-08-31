package bio.terra.stairway.impl;

import static bio.terra.stairway.FlightStatus.READY;
import static bio.terra.stairway.FlightStatus.READY_TO_RESTART;
import static bio.terra.stairway.FlightStatus.WAITING;

import bio.terra.stairway.Direction;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.impl.FlightContextImpl.StepRetry;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlightRunner executes the flight. It manages the atomic execution of a series of steps.
 */
public class FlightRunner implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(FlightRunner.class);

  private final FlightContextImpl flightContext;
  private final StairwayImpl stairway;
  private final HookWrapper hookWrapper;
  private final FlightDao flightDao;

  // Debug State
  // These sets will only be populated if the corresponding debugInfo field is populated.
  // If so, each set keeps track of which steps have already been failed so we only
  // fail them once.
  private final Set<Integer> debugStepsFailed;
  private final Set<String> debugDoStepsFailed;
  private final Set<String> debugUndoStepsFailed;

  public FlightRunner(FlightContextImpl flightContext) {
    this.flightContext = flightContext;
    // Dereference some commonly used objects
    stairway = flightContext.getStairwayImpl();
    hookWrapper = stairway.getHookWrapper();
    flightDao = stairway.getFlightDao();
    debugStepsFailed = new HashSet<>();
    debugDoStepsFailed = new HashSet<>();
    debugUndoStepsFailed = new HashSet<>();
  }

  public FlightContextImpl getFlightContext() {
    return flightContext;
  }

  /**
   * Execute the flight starting wherever the flight context says we are. We may be headed either
   * direction.
   */
  public void run() {
    try {
      hookWrapper.startFlight(flightContext);
      if (stairway.isQuietingDown()) {
        logger.info("Disowning flight starting during quietDown: " + flightContext.getFlightId());
        flightExit(READY);
        return;
      }

      logger.debug("Executing: " + flightContext.toString());
      FlightStatus flightStatus = fly();

      flightExit(flightStatus);
    } catch (InterruptedException ex) {
      // Shutdown - try disowning the flight
      logger.warn("Flight interrupted: " + flightContext.getFlightId());
      flightExit(READY);
    } catch (Exception ex) {
      logger.error("Flight failed with exception", ex);
    }
    try {
      hookWrapper.endFlight(flightContext);
    } catch (Exception ex) {
      logger.warn("End flight hook failed with exception", ex);
    }
  }

  /**
   * Common exit method that sets the flight state in the context and
   * tells stairway to perform state-related exit processing.
   *
   * @param flightStatus status of the flight
   */
  private void flightExit(FlightStatus flightStatus) {
    try {
      flightContext.setFlightStatus(flightStatus);
      stairway.exitFlight(flightContext);
    } catch (Exception ex) {
      logger.error("Failed to exit flight cleanly", ex);
    }
  }

  /**
   * Perform the flight, until we do all steps, undo to the beginning, or declare a dismal failure.
   */
  private FlightStatus fly() throws InterruptedException {
    try {
      flightContext.nextStepIndex(); // position the flight to execute the next thing

      // Part 1 - running forward (doing). We either succeed or we record the failure and
      // fall through to running backward (undoing)
      if (flightContext.isDoing()) {
        StepResult doResult = runSteps();
        if (doResult.isSuccess()) {
          if (doResult.getStepStatus() == StepStatus.STEP_RESULT_STOP) {
            return READY;
          }
          if (doResult.getStepStatus() == StepStatus.STEP_RESULT_WAIT) {
            return WAITING;
          }
          if (doResult.getStepStatus() == StepStatus.STEP_RESULT_RESTART_FLIGHT) {
            return READY_TO_RESTART;
          }
          return FlightStatus.SUCCESS;
        }

        // Remember the failure from the do; that is what we want to return
        // after undo completes
        flightContext.setResult(doResult);
        flightContext.setDirection(Direction.SWITCH);

        // Record the step failure and direction change in the database
        flightDao.step(flightContext);
      }

      // Part 2 - running backwards. We either succeed and return the original failure
      // status or we have a 'dismal failure'
      StepResult undoResult = runSteps();
      if (undoResult.isSuccess()) {
        // Return the error from the doResult - that is why we failed
        return FlightStatus.ERROR;
      }

      // Part 3 - dismal failure - undo failed!
      // Record the undo failure
      flightDao.step(flightContext);
      flightContext.setResult(undoResult);
      logger.error(
          "DISMAL FAILURE: non-retry-able error during undo. Flight: {}({}) Step: {}({})",
          flightContext.getFlightId(),
          flightContext.getFlightClassName(),
          flightContext.getStepIndex(),
          flightContext.getStepClassName());

    } catch (InterruptedException ex) {
      // Interrupted exception - we assume this means that the thread pool is shutting down and
      // forcibly stopping all threads. We propagate the exception.
      throw ex;
    } catch (Exception ex) {
      logger.error("Unhandled flight exception", ex);
      flightContext.setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex));
    }

    return FlightStatus.FATAL;
  }

  /**
   * run the steps in sequence, either forward or backward, until either we complete successfully or
   * we encounter an error. Note that this only records the step in the database if there is
   * success. Otherwise, it returns out and lets the outer logic setup the failure state before
   * recording it into the database.
   *
   * @return StepResult recording the success or failure of the most recent step
   * @throws InterruptedException on thread pool shutdown
   * @throws StairwayException some stairway exception
   */
  private StepResult runSteps() throws InterruptedException, StairwayException {
    // Initialize with current result, in case we are all done already
    StepResult result = flightContext.getResult();

    while (flightContext.haveStepToDo()) {
      result = stepWithRetry();

      // Exit if we hit a failure (result shows failed)
      if (!result.isSuccess()) {
        return result;
      }

      // If we SWITCHed from do to undo, make the direction UNDO
      if (flightContext.getDirection() == Direction.SWITCH) {
        flightContext.setDirection(Direction.UNDO);
      }

      if (getDebugRestartEachStep()) {
        StepResult newResult =
            new StepResult(
                StepStatus.STEP_RESULT_RESTART_FLIGHT, result.getException().orElse(null));
        flightDao.step(flightContext);
        return newResult;
      }
      switch (result.getStepStatus()) {
        case STEP_RESULT_SUCCESS:
          // Finished a step; run the next one
          flightContext.setRerun(false);
          flightDao.step(flightContext);
          flightContext.nextStepIndex();
          break;

        case STEP_RESULT_RERUN:
          // Rerun the same step
          flightContext.setRerun(true);
          flightDao.step(flightContext);
          break;

        case STEP_RESULT_WAIT:
          // Finished a step; yield execution
          flightContext.setRerun(false);
          flightDao.step(flightContext);
          return result;

        case STEP_RESULT_STOP:
          // Stop executing - leave rerun setting as is; we'll need to pick up where we left off
          flightDao.step(flightContext);
          return result;

        case STEP_RESULT_FAILURE_RETRY:
        case STEP_RESULT_FAILURE_FATAL:
        default:
          throw new StairwayExecutionException("Unexpected step status: " + result.getStepStatus());
      }
    }
    return result;
  }

  private StepResult stepWithRetry() throws InterruptedException, StairwayExecutionException {
    logger.debug("Executing " + flightContext.prettyStepState());

    StepRetry currentStepRetry = flightContext.getCurrentStepRetry();
    Step step = currentStepRetry.getStep();
    RetryRule retryRule = currentStepRetry.getRetryRule();

    StepResult result;

    // Retry loop
    do {
      try {
        // Do or undo based on direction we are headed
        hookWrapper.startStep(flightContext);

        if (flightContext.isDoing()) {
          result = step.doStep(flightContext);
          result = debugStatusReplacement(result);
        } else {
          result = step.undoStep(flightContext);
          result = debugStatusReplacement(result);
        }
      } catch (InterruptedException ex) {
        // Interrupted exception - we assume this means that the thread pool is shutting down and
        // forcibly stopping all threads. We propagate the exception.
        throw ex;
      } catch (Exception ex) {
        // The purpose of this catch is to relieve steps of implementing their own repetitive
        // try-catch simply to turn exceptions into StepResults.
        logger.info("Caught exception: (" + ex.toString() + ") " + flightContext.prettyStepState(), ex);

        // If the exception is the special Stairway RetryException, then we perform a retry.
        // Otherwise, it is an error.
        StepStatus stepStatus =
            (ex instanceof RetryException)
                ? StepStatus.STEP_RESULT_FAILURE_RETRY
                : StepStatus.STEP_RESULT_FAILURE_FATAL;
        result = new StepResult(stepStatus, ex);
      } finally {
        hookWrapper.endStep(flightContext);
      }

      switch (result.getStepStatus()) {
        case STEP_RESULT_SUCCESS:
        case STEP_RESULT_RERUN:
          if (stairway.isQuietingDown()) {
            // If we are quieting down, we force a stop
            result = new StepResult(StepStatus.STEP_RESULT_STOP, null);
          }
          return result;

        case STEP_RESULT_FAILURE_FATAL:
        case STEP_RESULT_STOP:
        case STEP_RESULT_WAIT:
        case STEP_RESULT_RESTART_FLIGHT:
          return result;

        case STEP_RESULT_FAILURE_RETRY:
          if (stairway.isQuietingDown()) {
            logger.info("Quieting down - not retrying: " + flightContext.prettyStepState());
            return result;
          }
          logger.info("Invoking retry rule: " + flightContext.prettyStepState());
          break;

        default:
          // Invalid step status returned from a step!
          throw new StairwayExecutionException(
              "Invalid step status returned: "
                  + result.getStepStatus()
                  + flightContext.prettyStepState());
      }

      // Retry case: the retry rule decides if we should try again or not.
    } while (retryRule.retrySleep());
    return result;
  }

  // -- Debug Support methods --

  // Are we restarting each step?
  private boolean getDebugRestartEachStep() {
    return (flightContext.getDebugInfo() != null
        && flightContext.getDebugInfo().getRestartEachStep());
  }

  /**
   * Helper function to replace the StepResult returned by a step based on {@link FlightDebugInfo}.
   *
   * @param initialResult the StepResult initially returned by the step.
   * @return StepResult to use for the step. May be the initial result.
   */
  private StepResult debugStatusReplacement(StepResult initialResult) {
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
    if (debugInfo.getLastStepFailure() && flightContext.isDoingLastStep()) {
      logger.info("Failed for debug mode last step failure.");
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
    }
    return initialResult;
  }

  // TODO: make toString
}
