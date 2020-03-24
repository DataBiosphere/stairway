package bio.terra.stairway;

public enum StepStatus {
    STEP_RESULT_SUCCESS,       // step succeeded, ready for next step
    STEP_RESULT_YIELD,         // step succeeded and yields the thread; for long waits
    STEP_RESULT_STOP,          // step succeeded; execution is quieting down
    STEP_RESULT_RERUN,         // step succeeded; rerun the step
    STEP_RESULT_FAILURE_RETRY, // step failed and is retryable
    STEP_RESULT_FAILURE_FATAL; // step failed and cannot be retried
}
