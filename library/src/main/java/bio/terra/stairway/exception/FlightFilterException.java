package bio.terra.stairway.exception;

public class FlightFilterException extends StairwayException {
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
