package bio.terra.stairway;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ProgressMeterData {
  private final String name;
  private final long v1;
  private final long v2;

  @JsonCreator
  public ProgressMeterData(
      @JsonProperty("name") String name,
      @JsonProperty("v1") long v1,
      @JsonProperty("v2")long v2) {
    this.name = name;
    this.v1 = v1;
    this.v2 = v2;
  }

  public String getName() {
    return name;
  }

  public long getV1() {
    return v1;
  }

  public long getV2() {
    return v2;
  }
}
