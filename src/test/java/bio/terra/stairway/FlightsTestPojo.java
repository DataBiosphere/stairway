package bio.terra.stairway;

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
}
