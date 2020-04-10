package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Step result objects are returned from the do() and undo() methods of a step.
 * The step status lets the Flight code know whether to continue forward,
 * go backward, or retry the step.
 */
public class StepResult {
    private static final Logger logger = LoggerFactory.getLogger(StepResult.class);

    private StepStatus stepStatus;
    private Exception exception;

    private static StepResult stepResultSuccess = new StepResult(StepStatus.STEP_RESULT_SUCCESS);
    /**
     * Static version of success result, so every step doesn't have to make one
     * @return returns a static StepResult of STEP_RESULT_SUCCESS
     */
    public static StepResult getStepResultSuccess() {
        return stepResultSuccess;
    }

    public StepResult(StepStatus stepStatus, Exception exception) {
        this.stepStatus = stepStatus;
        this.exception = exception;
        if (exception != null) {
            logger.error("Step Result exception", exception);
        }
    }

    // NOTE: This constructor is used in the DAO when deserializing the exception; we don't want to re-log it
    StepResult(Exception exception) {
        this.stepStatus = StepStatus.STEP_RESULT_FAILURE_FATAL;
        this.exception = exception;
    }

    public StepResult(StepStatus stepStatus) {
        this.stepStatus = stepStatus;
    }

    public StepStatus getStepStatus() {
        return stepStatus;
    }

    public Optional<Exception> getException() {
        return Optional.ofNullable(exception);
    }

    public boolean isSuccess() {
        return (stepStatus == StepStatus.STEP_RESULT_SUCCESS ||
                stepStatus == StepStatus.STEP_RESULT_RERUN ||
                stepStatus == StepStatus.STEP_RESULT_STOP ||
                stepStatus == StepStatus.STEP_RESULT_WAIT);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("stepStatus", stepStatus)
                .append("exception", exception)
                .toString();
    }
}
