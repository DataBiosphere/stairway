package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayException;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TestUtil {
    private TestUtil() {
    }

    static final Integer intValue = Integer.valueOf(22);
    static final String strValue = "testing 1 2 3";
    static final Double dubValue = new Double(Math.PI);
    static final String errString = "Something bad happened";
    static final String flightId = "aaa111";
    public static final String ikey = "ikey";
    static final String skey = "skey";
    static final String fkey = "fkey";
    static final String wikey = "wikey";
    static final String wskey = "wskey";
    static final String wfkey = "wfkey";

    static DataSource makeDataSource() {
        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName("org.postgresql.Driver");
        bds.setUrl(getEnvVar("STAIRWAY_URI", "jdbc:postgresql://127.0.0.1:5432/stairwaylib"));
        bds.setUsername(getEnvVar("STAIRWAY_USERNAME", "stairwayuser"));
        bds.setPassword(getEnvVar("STAIRWAY_PASSWORD", "stairwaypw"));
        return bds;
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    static Stairway setupStairway() throws Exception {
        DataSource dataSource = makeDataSource();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Stairway stairway = new Stairway(executorService, null);
        stairway.initialize(dataSource, true, true);
        return stairway;
    }

    static boolean isDone(Stairway stairway, String flightId) throws StairwayException {
        return stairway.getFlightState(flightId).getFlightStatus() != FlightStatus.RUNNING;
    }

    static Stairway setupDummyStairway() throws Exception {
        DataSource dataSource = makeDataSource();
        Stairway stairway = new Stairway(null, null);
        stairway.initialize(dataSource, true, true);
        return stairway;
    }
}
