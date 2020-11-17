package bio.terra.stairway.exception;

/**
 * Exception thrown when a flightID that's already in use is submitted.
 *
 * <p>Because flightIDs are specified by clients, it's useful to distinguish this case from other
 * database errors.
 */
public class DuplicateFlightIdSubmittedException extends StairwayException {
  public DuplicateFlightIdSubmittedException(String message) {
    super(message);
  }

  public DuplicateFlightIdSubmittedException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuplicateFlightIdSubmittedException(Throwable cause) {
    super(cause);
  }
}
