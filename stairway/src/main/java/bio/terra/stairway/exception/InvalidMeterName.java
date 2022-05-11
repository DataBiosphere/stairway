package bio.terra.stairway.exception;

/** Invalid flight filter provided to flight enumeration */
public class InvalidMeterName extends StairwayBadRequestException {

  public InvalidMeterName(String message) {
    super(message);
  }

  public InvalidMeterName(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidMeterName(Throwable cause) {
    super(cause);
  }
}
