package bio.terra.stairway.exception;

public abstract class StairwayInternalException extends StairwayException {
  public StairwayInternalException(String message) {
    super(message);
  }

  public StairwayInternalException(String message, Throwable cause) {
    super(message, cause);
  }

  public StairwayInternalException(Throwable cause) {
    super(cause);
  }
}
