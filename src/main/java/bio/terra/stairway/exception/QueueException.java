package bio.terra.stairway.exception;

public class QueueException extends StairwayException {
  public QueueException(String message) {
    super(message);
  }

  public QueueException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueueException(Throwable cause) {
    super(cause);
  }
}
