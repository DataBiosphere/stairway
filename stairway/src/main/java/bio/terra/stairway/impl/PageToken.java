package bio.terra.stairway.impl;

import bio.terra.stairway.exception.InvalidPageToken;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

/** Container/converter for a page token */
public class PageToken {
  public static final String PAGE_TOKEN_VERSION = "v01";
  private final Instant timestamp;

  public PageToken(Instant timestamp) {
    this.timestamp = timestamp;
  }

  public PageToken(String token) {
    if (!StringUtils.startsWith(token, PAGE_TOKEN_VERSION)) {
      throw new InvalidPageToken("Invalid page token");
    }

    try {
      String encodedInstant = StringUtils.removeStart(token, PAGE_TOKEN_VERSION);
      String base64String = URLDecoder.decode(encodedInstant, StandardCharsets.UTF_8);
      String instantString =
          new String(Base64.getDecoder().decode(base64String), StandardCharsets.UTF_8);
      timestamp = Instant.parse(instantString);
    } catch (IllegalArgumentException | DateTimeParseException e) {
      throw new InvalidPageToken("Invalid page token");
    }
  }

  public String makeToken() {
    String base64String =
        new String(
            Base64.getEncoder().encode(timestamp.toString().getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8);
    return PAGE_TOKEN_VERSION + URLEncoder.encode(base64String, StandardCharsets.UTF_8);
  }

  public Instant getTimestamp() {
    return timestamp;
  }
}
