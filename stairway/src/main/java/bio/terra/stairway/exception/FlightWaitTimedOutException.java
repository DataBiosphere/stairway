package bio.terra.stairway.exception;

public class FlightWaitTimedOutException extends StairwayBadRequestException {
  public FlightWaitTimedOutException(String message) {
    super(message);
  }

  public FlightWaitTimedOutException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlightWaitTimedOutException(Throwable cause) {
    super(cause);
  }
}
