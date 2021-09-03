package bio.terra.stairway.exception;

/** Invalid flight filter provided to flight enumeration */
public class FlightFilterException extends StairwayBadRequestException {

  public FlightFilterException(String message) {
    super(message);
  }

  public FlightFilterException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlightFilterException(Throwable cause) {
    super(cause);
  }
}
