package bio.terra.stairway.exception;

/** Flight was not found on lookup */
public class FlightNotFoundException extends StairwayBadRequestException {
  public FlightNotFoundException(String message) {
    super(message);
  }

  public FlightNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlightNotFoundException(Throwable cause) {
    super(cause);
  }
}
