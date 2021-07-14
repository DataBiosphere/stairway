package bio.terra.stairway.exception;

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
