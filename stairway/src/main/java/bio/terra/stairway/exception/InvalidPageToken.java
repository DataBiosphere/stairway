package bio.terra.stairway.exception;

/** Invalid flight filter provided to flight enumeration */
public class InvalidPageToken extends StairwayBadRequestException {

  public InvalidPageToken(String message) {
    super(message);
  }

  public InvalidPageToken(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidPageToken(Throwable cause) {
    super(cause);
  }
}
