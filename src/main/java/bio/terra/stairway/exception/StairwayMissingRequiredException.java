package bio.terra.stairway.exception;

public class StairwayMissingRequiredException extends StairwayRuntimeException {
    public StairwayMissingRequiredException(String message) {
        super(message);
    }

    public StairwayMissingRequiredException(String message, Throwable cause) {
        super(message, cause);
    }

    public StairwayMissingRequiredException(Throwable cause) {
        super(cause);
    }
}
