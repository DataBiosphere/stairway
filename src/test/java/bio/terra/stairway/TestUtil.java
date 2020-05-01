package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

public final class TestUtil {
    private static Logger logger = LoggerFactory.getLogger(TestUtil.class);

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

    public static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    static String randomStairwayName() {
        return "test_" + ShortUUID.get();
    }

    static Stairway setupDefaultStairway() throws Exception {
        return setupStairway(randomStairwayName(), false);
    }

    static Stairway setupStairway(String stairwayName, boolean continuing) throws Exception {
        return makeStairway(stairwayName, !continuing, !continuing, null);
    }

    static Stairway setupConnectedStairway(String stairwayName, boolean continuing) throws Exception {
        String projectId = getEnvVar("GOOGLE_CLOUD_PROJECT", null);
        if (projectId == null) {
            throw new IllegalStateException("You must have GOOGLE_CLOUD_PROJECT and " +
                    "GOOGLE_APPLICATION_CREDENTIALS envvars defined");
        }
        return makeStairway(stairwayName, !continuing, !continuing, projectId);
    }

    static void sleepStop() throws InterruptedException {
        if (TestStopController.getControl() == 0) {
            logger.debug("sleepStop stopping");
            TimeUnit.HOURS.sleep(1);
        }
        logger.debug("sleepStop did not stop");
    }

    private static Stairway makeStairway(String stairwayName,
                                         boolean forceCleanStart,
                                         boolean migrateUpgrade,
                                         String projectId) throws Exception {
        DataSource dataSource = makeDataSource();
        Stairway stairway = Stairway.newBuilder()
                .stairwayClusterName("stairway-cluster")
                .stairwayName(stairwayName)
                .projectId(projectId)
                .maxParallelFlights(2)
                .build();
        stairway.initialize(dataSource, forceCleanStart, migrateUpgrade);
        return stairway;
    }

    static boolean isDone(Stairway stairway, String flightId) throws StairwayException, InterruptedException{
        return stairway.getFlightState(flightId).getFlightStatus() != FlightStatus.RUNNING;
    }

}
