package bio.terra.stairway;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO that represents the data for one project meter. A project meter is named and has two data
 * values. The values are typically used to represent progress in the form of: N things done out of
 * M total things.
 */
public class ProgressMeterData {
  private final long v1;
  private final long v2;

  @JsonCreator
  public ProgressMeterData(@JsonProperty("v1") long v1, @JsonProperty("v2") long v2) {
    this.v1 = v1;
    this.v2 = v2;
  }

  public long getV1() {
    return v1;
  }

  public long getV2() {
    return v2;
  }
}
