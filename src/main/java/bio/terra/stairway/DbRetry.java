package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The DbRetry class provides a common retry framework for the DAOs. A new DbRetry class is
 * constructed at runtime for use wrapping inner database calls. The retry algorithm is random
 * backoff with a maximum number of retries. At this time, there is no use case for setting
 * different retry times for different calls. If there is, then an additional constructor can be
 * added to allow specification of what are coded as statics.
 */
public class DbRetry {
  private static final Logger logger = LoggerFactory.getLogger(DbRetry.class);
  private static final int MAX_RETRIES = 20;
  private static final int WAIT_MIN_MS = 250;
  private static final int WAIT_MAX_MS = 1000;
  private static final String PSQL_SERIALIZATION_FAILURE = "40001";
  private static final String PSQL_DEADLOCK_DETECTED = "40P01";
  private static final String PSQL_CONNECTION_ISSUE_PREFIX = "08";
  private static final String PSQL_RESOURCE_ISSUE_PREFIX = "53";

  private final String logString;

  public DbRetry(String logString) {
    this.logString = logString;
  }

  <T> T perform(DbFunction<T> function) throws DatabaseOperationException, InterruptedException {
    for (int retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        return function.apply();
      } catch (SQLException ex) {
        if (!isRetryable(ex)) {
          throw new DatabaseOperationException("Database operation failed: " + logString, ex);
        }
        retryWait();
      }
    }
    throw new DatabaseOperationException("Retries exhausted. Request failed:" + logString);
  }

  private boolean isRetryable(SQLException ex) {
    final String ss = ex.getSQLState();

    if (ss.equals(PSQL_SERIALIZATION_FAILURE) || ss.equals(PSQL_DEADLOCK_DETECTED)) {
      // Serialization or deadlock
      logger.info("Caught SQL serialization error (" + ss + ") - retrying");
      return true;
    }
    if (ss.startsWith(PSQL_CONNECTION_ISSUE_PREFIX) || ss.startsWith(PSQL_RESOURCE_ISSUE_PREFIX)) {
      // Connection or resource issue
      logger.info("Caught SQL connection or resource error (" + ss + ") - retrying");
      return true;
    }
    return false;
  }

  private void retryWait() throws InterruptedException {
    int sleepMS = ThreadLocalRandom.current().nextInt(WAIT_MIN_MS, WAIT_MAX_MS);
    TimeUnit.MILLISECONDS.sleep(sleepMS);
    logger.info("retrying for {}", logString);
  }
}
