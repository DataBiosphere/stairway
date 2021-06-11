package bio.terra.stairway;

public enum StepStatus {
  STEP_RESULT_SUCCESS, // step succeeded, ready for next step
  STEP_RESULT_WAIT, // step succeeded; the flight waits for a completion event
  STEP_RESULT_STOP, // step succeeded; execution is quieting down
  STEP_RESULT_RERUN, // step succeeded; rerun the step
  STEP_RESULT_FAILURE_RETRY, // step failed and is retryable
  STEP_RESULT_FAILURE_FATAL, // step failed and cannot be retried
  STEP_RESULT_RESTART_FLIGHT; // Running in restartEachStep Mode. This step was successful but
  // flight needs to be restarted.
}
