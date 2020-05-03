package bio.terra.stairway.exception;

/**
 * StairwayExecutionException indicates that something is wrong in the Stairway execution code; an
 * invalid state or similar.
 */
public class MigrateException extends StairwayException {
  public MigrateException(String message) {
    super(message);
  }

  public MigrateException(String message, Throwable cause) {
    super(message, cause);
  }

  public MigrateException(Throwable cause) {
    super(cause);
  }
}
