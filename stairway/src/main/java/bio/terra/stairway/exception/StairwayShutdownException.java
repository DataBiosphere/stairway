package bio.terra.stairway.exception;

public class StairwayShutdownException extends StairwayBadRequestException {
  public StairwayShutdownException(String message) {
    super(message);
  }

  public StairwayShutdownException(String message, Throwable cause) {
    super(message, cause);
  }

  public StairwayShutdownException(Throwable cause) {
    super(cause);
  }
}
