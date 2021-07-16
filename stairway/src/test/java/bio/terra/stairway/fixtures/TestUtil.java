package bio.terra.stairway.fixtures;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.StairwayException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtil {
  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

  private TestUtil() {}

  public static final int intValue = 22;
  public static final String strValue = "testing 1 2 3";
  public static final double dubValue = Math.PI;
  public static final String errString = "Something bad happened";
  public static final String flightId = "aaa111";
  public static final String ikey = "ikey";
  public static final String skey = "skey";
  public static final String fkey = "fkey";
  public static final String wikey = "wikey";
  public static final String wskey = "wskey";
  public static final String wfkey = "wfkey";

  public static DataSource makeDataSource() {
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

  // Optionally pauses a flight in the middle so we can fake failures
  public static void sleepPause() throws InterruptedException {
    if (TestPauseController.getControl() == 0) {
      logger.debug("sleepPause sleeping");
      TimeUnit.HOURS.sleep(1);
    }
    logger.debug("sleepStop did not stop");
  }

  public static boolean isDone(Stairway stairway, String flightId)
      throws StairwayException, InterruptedException {
    return stairway.getFlightState(flightId).getFlightStatus() != FlightStatus.RUNNING;
  }

  /**
   * Get the project id from the environment. If it is not set we bail. This is used in connected
   * tests to make sure we are connected.
   *
   * @return projectId
   */
  public static String getProjectId() {
    String projectId = TestUtil.getEnvVar("GOOGLE_CLOUD_PROJECT", null);
    if (projectId == null) {
      throw new IllegalStateException(
          "You must have GOOGLE_CLOUD_PROJECT and "
              + "GOOGLE_APPLICATION_CREDENTIALS envvars defined");
    }
    return projectId;
  }

  public static void checkHookLog(List<String> compareHookLog) {
    List<String> hookLog = TestHook.getHookLog();
    assertThat("logs are the same size", hookLog.size(), equalTo(compareHookLog.size()));

    for (int i = 0; i < hookLog.size(); i++) {
      assertThat("Log lines match", StringUtils.equals(hookLog.get(i), compareHookLog.get(i)));
    }
  }
}
