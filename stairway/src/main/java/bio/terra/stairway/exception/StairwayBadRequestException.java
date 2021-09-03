package bio.terra.stairway.exception;

/** Base class for caller request errors; equivalent to HTTP 400 errors */
public abstract class StairwayBadRequestException extends StairwayException {
  public StairwayBadRequestException(String message) {
    super(message);
  }

  public StairwayBadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  public StairwayBadRequestException(Throwable cause) {
    super(cause);
  }
}
