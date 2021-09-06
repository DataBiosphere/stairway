package bio.terra.stairway.exception;

/** Failure to deserialize data stored as a JSON string */
public class JsonConversionException extends StairwayInternalException {
  public JsonConversionException(String message) {
    super(message);
  }

  public JsonConversionException(String message, Throwable cause) {
    super(message, cause);
  }

  public JsonConversionException(Throwable cause) {
    super(cause);
  }
}
