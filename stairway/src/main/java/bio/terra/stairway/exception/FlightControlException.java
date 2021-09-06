package bio.terra.stairway.exception;

/**
 * Base class for convenience exceptions used to control flight behavior. At the time of this
 * writing, there is only one such exception: {@link RetryException}
 */
public abstract class FlightControlException extends StairwayException {
  public FlightControlException(String message) {
    super(message);
  }

  public FlightControlException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlightControlException(Throwable cause) {
    super(cause);
  }
}
