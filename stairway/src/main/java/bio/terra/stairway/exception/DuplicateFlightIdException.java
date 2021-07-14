package bio.terra.stairway.exception;

/**
 * Exception thrown when a flightID that's already in use is submitted.
 *
 * <p>Because flightIDs are specified by clients, it's useful to distinguish this case from other
 * database errors.
 */
public class DuplicateFlightIdException extends StairwayBadRequestException {
  public DuplicateFlightIdException(String message) {
    super(message);
  }

  public DuplicateFlightIdException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuplicateFlightIdException(Throwable cause) {
    super(cause);
  }
}
