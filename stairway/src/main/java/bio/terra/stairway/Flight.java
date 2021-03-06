package bio.terra.stairway;

import static bio.terra.stairway.FlightStatus.READY;
import static bio.terra.stairway.FlightStatus.READY_TO_RESTART;
import static bio.terra.stairway.FlightStatus.WAITING;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.RetryException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage the atomic execution of a series of Steps This base class has the mechanisms for executing
 * the series of steps.
 *
 * <p>In order for the flight to be re-created on recovery, the construction and configuration have
 * to result in the same flight given the same input.
 */
public class Flight implements Runnable {
  static class StepRetry {
    private final Step step;
    private final RetryRule retryRule;

    StepRetry(Step step, RetryRule retryRule) {
      this.step = step;
      this.retryRule = retryRule;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Flight.class);

  private final List<StepRetry> steps;
  private final List<String> stepClassNames;
  private FlightDao flightDao;
  private FlightContext flightContext;
  private final Object applicationContext;

  // These sets will only be populated if the corresponding debugInfo field is populated. If so,
  // each set keeps track of which steps have already been failed so we do not infinitely
  // retry.
  private final Set<Integer> debugStepsFailed;
  private final Set<String> debugDoStepsFailed;
  private final Set<String> debugUndoStepsFailed;

  public Flight(FlightMap inputParameters, Object applicationContext) {
    this.applicationContext = applicationContext;
    steps = new LinkedList<>();
    stepClassNames = new LinkedList<>();
    flightContext = new FlightContext(inputParameters, this.getClass().getName(), stepClassNames);
    debugStepsFailed = new HashSet<>();
    debugDoStepsFailed = new HashSet<>();
    debugUndoStepsFailed = new HashSet<>();
  }

  public void setDebugInfo(FlightDebugInfo debugInfo) {
    this.context().setDebugInfo(debugInfo);
  }

  public HookWrapper hookWrapper() {
    return context().getStairway().getHookWrapper();
  }

  public FlightContext context() {
    return flightContext;
  }

  public Object getApplicationContext() {
    return applicationContext;
  }

  public void setFlightContext(FlightContext flightContext) {
    flightContext.setStepClassNames(stepClassNames);
    this.flightContext = flightContext;
  }

  // Used by subclasses to build the step list with default no-retry rule
  protected void addStep(Step step) {
    addStep(step, RetryRuleNone.getRetryRuleNone());
  }

  // Used by subclasses to build the step list with a retry rule
  protected void addStep(Step step, RetryRule retryRule) {
    steps.add(new StepRetry(step, retryRule));
    stepClassNames.add(step.getClass().getName());
  }

  /**
   * Execute the flight starting wherever the flight context says we are. We may be headed either
   * direction.
   */
  public void run() {
    try {
      hookWrapper().startFlight(flightContext);

      // We use flightDao all over the place, so we put it in a private to avoid passing it through
      // all of the method argument lists.
      flightDao = context().getStairway().getFlightDao();

      if (context().getStairway().isQuietingDown()) {
        logger.info("Disowning flight starting during quietDown: " + context().getFlightId());
        flightExit(READY);
        return;
      }

      logger.debug("Executing: " + context().toString());
      FlightStatus flightStatus = fly();
      flightExit(flightStatus);
    } catch (InterruptedException ex) {
      // Shutdown - try disowning the flight
      logger.warn("Flight interrupted: " + context().getFlightId());
      flightExit(READY);
    } catch (Exception ex) {
      logger.error("Flight failed with exception", ex);
    }
    try {
      hookWrapper().endFlight(flightContext);
    } catch (Exception ex) {
      logger.warn("End flight hook failed with exception", ex);
    }
  }

  private void flightExit(FlightStatus flightStatus) {
    try {
      context().setFlightStatus(flightStatus);
      context().getStairway().exitFlight(context());
    } catch (Exception ex) {
      logger.error("Failed to exit flight cleanly", ex);
    }
  }

  /**
   * Perform the flight, until we do all steps, undo to the beginning, or declare a dismal failure.
   */
  private FlightStatus fly() throws InterruptedException {
    try {
      context().nextStepIndex(); // position the flight to execute the next thing

      // Part 1 - running forward (doing). We either succeed or we record the failure and
      // fall through to running backward (undoing)
      if (context().isDoing()) {
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
        context().setResult(doResult);
        context().setDirection(Direction.SWITCH);

        // Record the step failure and direction change in the database
        flightDao.step(context());
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
      flightDao.step(context());
      context().setResult(undoResult);
      logger.error(
          "DISMAL FAILURE: non-retry-able error during undo. Flight: {}({}) Step: {}({})",
          context().getFlightId(),
          context().getFlightClassName(),
          context().getStepIndex(),
          context().getStepClassName());

    } catch (InterruptedException ex) {
      // Interrupted exception - we assume this means that the thread pool is shutting down and
      // forcibly stopping all threads. We propagate the exception.
      throw ex;
    } catch (Exception ex) {
      logger.error("Unhandled flight exception", ex);
      context().setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex));
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
   */
  private StepResult runSteps()
      throws InterruptedException, StairwayExecutionException, DatabaseOperationException {
    // Initialize with current result, in case we are all done already
    StepResult result = context().getResult();

    while (context().haveStepToDo(steps.size())) {
      result = stepWithRetry();

      // Exit if we hit a failure (result shows failed)
      if (!result.isSuccess()) {
        return result;
      }

      // If we SWITCHed from do to undo, make the direction UNDO
      if (context().getDirection() == Direction.SWITCH) {
        context().setDirection(Direction.UNDO);
      }

      if (context().getDebugInfo() != null && this.context().getDebugInfo().getRestartEachStep()) {
        StepResult newResult =
            new StepResult(
                StepStatus.STEP_RESULT_RESTART_FLIGHT, result.getException().orElse(null));
        flightDao.step(context());
        return newResult;
      }
      switch (result.getStepStatus()) {
        case STEP_RESULT_SUCCESS:
          // Finished a step; run the next one
          context().setRerun(false);
          flightDao.step(context());
          context().nextStepIndex();
          break;

        case STEP_RESULT_RERUN:
          // Rerun the same step
          context().setRerun(true);
          flightDao.step(context());
          break;

        case STEP_RESULT_WAIT:
          // Finished a step; yield execution
          context().setRerun(false);
          flightDao.step(context());
          return result;

        case STEP_RESULT_STOP:
          // Stop executing - leave rerun setting as is; we'll need to pick up where we left off
          flightDao.step(context());
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
    logger.debug("Executing " + context().prettyStepState());

    StepRetry currentStep = getCurrentStep();
    currentStep.retryRule.initialize();

    StepResult result;

    // Retry loop
    do {
      try {
        // Do or undo based on direction we are headed
        hookWrapper().startStep(flightContext);

        if (context().isDoing()) {
          result = currentStep.step.doStep(context());
          result = debugStatusReplacement(result);
        } else {
          result = currentStep.step.undoStep(context());
          result = debugStatusReplacement(result);
        }
      } catch (InterruptedException ex) {
        // Interrupted exception - we assume this means that the thread pool is shutting down and
        // forcibly stopping all threads. We propagate the exception.
        throw ex;
      } catch (Exception ex) {
        // The purpose of this catch is to relieve steps of implementing their own repetitive
        // try-catch simply to turn exceptions into StepResults.
        logger.info("Caught exception: (" + ex.toString() + ") " + context().prettyStepState(), ex);

        StepStatus stepStatus =
            (ex instanceof RetryException)
                ? StepStatus.STEP_RESULT_FAILURE_RETRY
                : StepStatus.STEP_RESULT_FAILURE_FATAL;
        result = new StepResult(stepStatus, ex);
      } finally {
        hookWrapper().endStep(flightContext);
      }

      switch (result.getStepStatus()) {
        case STEP_RESULT_SUCCESS:
        case STEP_RESULT_RERUN:
          if (context().getStairway().isQuietingDown()) {
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
          if (context().getStairway().isQuietingDown()) {
            logger.info("Quieting down - not retrying: " + context().prettyStepState());
            return result;
          }
          logger.info("Invoking retry rule: " + context().prettyStepState());
          break;

        default:
          // Invalid step status returned from a step!
          throw new StairwayExecutionException(
              "Invalid step status returned: "
                  + result.getStepStatus()
                  + context().prettyStepState());
      }
    } while (currentStep.retryRule
        .retrySleep()); // retry rule decides if we should try again or not
    return result;
  }

  /**
   * Helper function to replace the StepResult returned by a step based on {@link FlightDebugInfo}.
   *
   * @param initialResult the StepResult initially returned by the step.
   * @return StepResult to use for the step. May be the initial result.
   */
  private StepResult debugStatusReplacement(StepResult initialResult) {
    if (context().getDebugInfo() == null) {
      return initialResult;
    }
    FlightDebugInfo debugInfo = context().getDebugInfo();
    // If we are in debug mode, in the DO and a failure is set for this step AND we have not already
    // failed here, then insert a failure. We do this right after the step completes but
    // before the flight logs it so that we can look for dangerous UNDOs.
    if (context().isDoing()
        && debugInfo.getFailAtSteps() != null
        && debugInfo.getFailAtSteps().containsKey(context().getStepIndex())
        && !debugStepsFailed.contains(context().getStepIndex())) {
      StepStatus failStatus = debugInfo.getFailAtSteps().get(context().getStepIndex());
      logger.info(
          "Failed for debug mode fail step at step {} with result {}",
          context().getStepIndex(),
          failStatus);
      debugStepsFailed.add(context().getStepIndex());
      return new StepResult(failStatus);
    }
    String currentStepClassName = context().getStepClassName();
    // If we are in debug mode, doing, and a a failure is set for this step class name for do and we
    // have not already failed for this Step class, then insert a failure.
    if (context().isDoing()
        && debugInfo.getDoStepFailures() != null
        && debugInfo.getDoStepFailures().containsKey(currentStepClassName)
        && !debugDoStepsFailed.contains(currentStepClassName)) {
      StepStatus failStatus = debugInfo.getDoStepFailures().get(currentStepClassName);
      logger.info(
          "Failed for debug mode fail do step at step {} with result {}",
          context().getStepIndex(),
          failStatus);
      debugDoStepsFailed.add(currentStepClassName);
      return new StepResult(failStatus);
    }
    // If we are in debug mode, undoing, and a a failure is set for this step class name for undo
    // and we have not already failed for this Step class, then insert a failure.
    if (!context().isDoing()
        && debugInfo.getUndoStepFailures() != null
        && debugInfo.getUndoStepFailures().containsKey(currentStepClassName)
        && !debugUndoStepsFailed.contains(currentStepClassName)) {
      StepStatus failStatus = debugInfo.getUndoStepFailures().get(currentStepClassName);
      logger.info(
          "Failed for debug mode fail do step at step {} with result {}",
          context().getStepIndex(),
          failStatus);
      debugUndoStepsFailed.add(currentStepClassName);
      return new StepResult(failStatus);
    }
    // If we are in debug mode for failing at the last step, and this is the last step, insert a
    // failure.
    if (context().isDoing()
        && debugInfo.getLastStepFailure()
        && context().getStepIndex() == steps.size() - 1) {
      logger.info("Failed for debug mode last step failure.");
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
    }
    return initialResult;
  }

  private StepRetry getCurrentStep() throws StairwayExecutionException {
    int stepIndex = context().getStepIndex();
    if (stepIndex < 0 || stepIndex >= steps.size()) {
      throw new StairwayExecutionException("Invalid step index: " + stepIndex);
    }

    return steps.get(stepIndex);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("steps", steps)
        .append("flightContext", flightContext)
        .toString();
  }
}
