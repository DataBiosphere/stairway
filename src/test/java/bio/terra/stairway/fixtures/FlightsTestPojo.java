package bio.terra.stairway.fixtures;

import java.util.Objects;

public class FlightsTestPojo {
  private String astring;
  private int anint;

  public String getAstring() {
    return astring;
  }

  public FlightsTestPojo astring(String astring) {
    this.astring = astring;
    return this;
  }

  public int getAnint() {
    return anint;
  }

  public FlightsTestPojo anint(int anint) {
    this.anint = anint;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlightsTestPojo) {
      FlightsTestPojo that = (FlightsTestPojo) obj;
      return this.astring.equals(that.astring) && this.anint == that.anint;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.astring, this.anint);
  }
}
