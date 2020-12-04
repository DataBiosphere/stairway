package bio.terra.stairway;

import static bio.terra.stairway.FlightStatus.READY;
import static bio.terra.stairway.FlightStatus.WAITING;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.RetryException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.LinkedList;
import java.util.List;
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

  public Flight(FlightMap inputParameters, Object applicationContext) {
    this.applicationContext = applicationContext;
    steps = new LinkedList<>();
    stepClassNames = new LinkedList<>();
    flightContext = new FlightContext(inputParameters, this.getClass().getName(), stepClassNames);
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
    hookWrapper().startFlight(flightContext);
    try {
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
      hookWrapper().endFlight(flightContext);
    } catch (InterruptedException ex) {
      // Shutdown - try disowning the flight
      logger.warn("Flight interrupted: " + context().getFlightId());
      flightExit(READY);
    } catch (Exception ex) {
      logger.error("Flight failed with exception", ex);
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
      // TODO: Not actually wired through HookWrapper, or backwards compatible,
      //  but would look something like this.
      StairwayHookV2.StepHook stepHook = hookWrapper().startStep(flightContext);
      stepHook.start(context());
      result = null;
      try {
        // Do or undo based on direction we are headed
        if (context().isDoing()) {
          result = currentStep.step.doStep(context());
        } else {
          result = currentStep.step.undoStep(context());
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
        stepHook.end(flightContext, result);
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
          return result;

        case STEP_RESULT_FAILURE_RETRY:
          if (context().getStairway().isQuietingDown()) {
            logger.info("Quieting down - not retrying: " + context().prettyStepState());
            return result;
          }
          logger.info("Retrying: " + context().prettyStepState());
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
