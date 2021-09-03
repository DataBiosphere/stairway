package bio.terra.stairway.exception;

/** Base class for all stairway exceptions */
public abstract class StairwayException extends RuntimeException {
  public StairwayException(String message) {
    super(message);
  }

  public StairwayException(String message, Throwable cause) {
    super(message, cause);
  }

  public StairwayException(Throwable cause) {
    super(cause);
  }
}
