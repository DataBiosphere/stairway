package bio.terra.stairway;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Simple container for a key-value pair. This is used for search predicates when enumerating
 * flights by input parameters. The key is the string key; the value is the json string of the
 * value.
 */
public class FlightInput {
  private String key;
  private String value;

  @JsonCreator
  public FlightInput(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }
}
