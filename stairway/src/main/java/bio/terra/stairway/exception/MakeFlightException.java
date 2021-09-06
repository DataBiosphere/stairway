package bio.terra.stairway.exception;

/** Stairway was unable to construct the client Flight subclass object. */
public class MakeFlightException extends StairwayBadRequestException {
  public MakeFlightException(String message) {
    super(message);
  }

  public MakeFlightException(String message, Throwable cause) {
    super(message, cause);
  }

  public MakeFlightException(Throwable cause) {
    super(cause);
  }
}
