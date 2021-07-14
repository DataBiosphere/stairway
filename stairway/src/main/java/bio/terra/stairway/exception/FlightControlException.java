package bio.terra.stairway.exception;

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
