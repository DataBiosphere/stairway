package bio.terra.stairway.exception;

/**
 * RetryException can be thrown by client step code to cause the step to be retried. It is
 * equivalent to the step returning STEP_STATUS_FAILURE_RETRY.
 */
public class RetryException extends FlightControlException {
  public RetryException(String message) {
    super(message);
  }

  public RetryException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetryException(Throwable cause) {
    super(cause);
  }
}
