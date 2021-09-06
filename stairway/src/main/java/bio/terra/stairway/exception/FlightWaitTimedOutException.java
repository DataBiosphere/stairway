package bio.terra.stairway.exception;

/** Thrown when {@link bio.terra.stairway.Stairway#waitForFlight} times out. */
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
