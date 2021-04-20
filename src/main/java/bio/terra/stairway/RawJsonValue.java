package bio.terra.stairway;

/**
 * Simple wrapper around a String containing the raw JSON representation of an object. This exists
 * in order to allow us to register a custom deserializer, which in turn can make use of Jackson
 * {@link com.fasterxml.jackson.core.JsonGenerator} calls to inject raw JSON at serialization time.
 * This is necessary because serializing String types will result in escaping that creates JSON
 * incompatible with earlier code's serialization of {@code Map<String, Object>}}.
 */
final class RawJsonValue {
  private final String jsonValue;

  public static RawJsonValue create(String jsonValue) {
    return new RawJsonValue(jsonValue);
  }

  /**
   * Creates a wrapper to hold the passed raw JSON.
   *
   * @param jsonValue JSON String to wrap.
   */
  private RawJsonValue(String jsonValue) {
    this.jsonValue = jsonValue;
  }

  /**
   * Gets a String containing raw JSON, suitable for use with {@link
   * com.fasterxml.jackson.core.JsonGenerator#writeRawValue(String)}.
   */
  public String getJsonValue() {
    return jsonValue;
  }
}
