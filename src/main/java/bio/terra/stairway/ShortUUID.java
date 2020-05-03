package bio.terra.stairway;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/**
 * UUIDs are very useful and very long: 36 characters. We can improve on this by 38% by writing the
 * UUID out in Base64url format instead of decimal digits with hyphens.
 */
public class ShortUUID {
  /**
   * Get a random UUID in base64url format.
   *
   * @return short, but just as unique, UUID string
   */
  public static String get() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer byteBuffer = ByteBuffer.allocate(16);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());

    // The replaceAll's are here to make the string URL and file system name friendly
    // without loss of uniqueness.
    return Base64.getEncoder()
        .withoutPadding()
        .encodeToString(byteBuffer.array())
        .replaceAll("/", "_")
        .replaceAll("\\+", "-");
  }
}
