package bio.terra.stairway.fixtures;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.ShortUUID;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.QueueException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
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

  public static final Integer intValue = 22;
  public static final String strValue = "testing 1 2 3";
  public static final Double dubValue = Math.PI;
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

  public static String randomStairwayName() {
    return "test_" + ShortUUID.get();
  }

  public static Stairway setupDefaultStairway() throws Exception {
    return setupStairway(randomStairwayName(), false);
  }

  public static Stairway setupStairway(String stairwayName, boolean continuing) throws Exception {
    return makeStairway(stairwayName, !continuing, !continuing, null, 0);
  }

  public static Stairway setupStairwayWithHooks(String stairwayName, boolean continuing, int hooks)
      throws Exception {
    return makeStairway(stairwayName, !continuing, !continuing, null, hooks);
  }

  public static Stairway setupConnectedStairway(String stairwayName, boolean continuing)
      throws Exception {
    return makeStairway(stairwayName, !continuing, !continuing, getProjectId(), 0);
  }

  public static Stairway setupConnectedStairwayWithHooks(
      String stairwayName, boolean continuing, int hooks) throws Exception {
    return makeStairway(stairwayName, !continuing, !continuing, getProjectId(), hooks);
  }

  // Optionally pauses a flight in the middle so we can fake failures
  public static void sleepPause() throws InterruptedException {
    if (TestPauseController.getControl() == 0) {
      logger.debug("sleepPause sleeping");
      TimeUnit.HOURS.sleep(1);
    }
    logger.debug("sleepStop did not stop");
  }

  private static Stairway makeStairway(
      String stairwayName,
      boolean forceCleanStart,
      boolean migrateUpgrade,
      String projectId,
      int hooks)
      throws Exception {
    DataSource dataSource = makeDataSource();
    boolean enableWorkQueue = (projectId != null);

    Stairway.Builder builder =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(projectId)
            .enableWorkQueue(enableWorkQueue)
            .maxParallelFlights(2);

    for (int i = 0; i < hooks; i++) {
      int hookId = i + 1;
      TestHook hook = new TestHook(Integer.toString(hookId));
      builder.stairwayHook(hook);
    }
    TestHook.clearHookLog();

    Stairway stairway = builder.build();

    List<String> recordedStairways =
        stairway.initialize(dataSource, forceCleanStart, migrateUpgrade);
    if (forceCleanStart) {
      assertThat("nothing to recover", recordedStairways.size(), equalTo(0));
    }
    stairway.recoverAndStart(recordedStairways);
    return stairway;
  }

  // For cases where we want to validate recovery of a single stairway instance and that
  // that a specific flight is READY and unowned.
  public static Stairway makeStairwayValidateRecovery(
      String stairwayName, String flightId, int hooks)
      throws DatabaseOperationException, QueueException, MigrateException,
          StairwayExecutionException, InterruptedException, DatabaseSetupException {
    DataSource dataSource = makeDataSource();

    Stairway.Builder builder =
        Stairway.newBuilder()
            .stairwayClusterName("stairway-cluster")
            .stairwayName(stairwayName)
            .workQueueProjectId(null)
            .maxParallelFlights(2);

    for (int i = 0; i < hooks; i++) {
      int hookId = i + 1;
      TestHook hook = new TestHook(Integer.toString(hookId));
      builder.stairwayHook(hook);
    }

    Stairway stairway = builder.build();

    List<String> recordedStairways = stairway.initialize(dataSource, false, false);
    assertThat("one stairway to recover", recordedStairways.size(), equalTo(1));
    assertThat("stairway name matches", recordedStairways.get(0), equalTo(stairwayName));

    FlightState state = stairway.getFlightState(flightId);
    assertThat("State is ready", state.getFlightStatus(), equalTo(FlightStatus.READY));
    assertNull(state.getStairwayId(), "Flight is unowned");

    stairway.recoverAndStart(recordedStairways);
    return stairway;
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
