package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightFilterException;
import bio.terra.stairway.exception.FlightNotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The general layout of the stairway database tables is:
 *
 * <ul>
 *   <li>flight - records the flight and its outputs if any
 *   <li>flight input - records the inputs of a flight in a query-ably form
 *   <li>flight log - records the steps of a running flight for recovery
 * </ul>
 *
 * This code assumes that the database is created and matches this codes schema expectations. If
 * not, we will crash and burn.
 *
 * <p>May want to split this into an interface and an implementation. This implementation has only
 * been tested on Postgres. It may work on other databases, but who knows?
 *
 * <p>The practice with transaction code is:
 *
 * <ul>
 *   <li>Always explicitly start the transaction. That ensures that all transactions are using the
 *       serializable isolation level.
 *   <li>Always explicitly declare read only vs write transactions
 *   <li>Always explicitly commit the transaction. That is not needed when using the resource try,
 *       but it is useful documentation to see the point where we expect the transaction to be
 *       complete.
 * </ul>
 */
@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
class FlightDao {
  private static final Logger logger = LoggerFactory.getLogger(FlightDao.class);

  static final String FLIGHT_TABLE = "flight";
  static final String FLIGHT_LOG_TABLE = "flightlog";
  static final String FLIGHT_INPUT_TABLE = "flightinput";
  static final String STAIRWAY_INSTANCE_TABLE = "stairwayinstance";
  private static final String UNKNOWN = "<unknown>";

  private final DataSource dataSource;
  private final ExceptionSerializer exceptionSerializer;
  private final boolean keepFlightLog;

  FlightDao(DataSource dataSource, ExceptionSerializer exceptionSerializer, boolean keepFlightLog) {
    this.dataSource = dataSource;
    this.exceptionSerializer = exceptionSerializer;
    this.keepFlightLog = keepFlightLog;
  }

  /**
   * Find or create a stairway instance. It is allowable to destroy and recreate a stairway instance
   * of the same name. It will recover flights that it owns.
   *
   * @param stairwayName string name of this stairway instance; must be unique across stairways in
   *     the database
   * @return short UUID for this stairway instance
   * @throws DatabaseOperationException on database errors
   */
  String findOrCreateStairwayInstance(String stairwayName)
      throws DatabaseSetupException, DatabaseOperationException {

    try (Connection connection = dataSource.getConnection()) {
      startTransaction(connection);

      String stairwayId = lookupStairwayInstanceQuery(connection, stairwayName);
      if (stairwayId != null) {
        return stairwayId;
      }

      final String sqlStairwayInstanceCreate =
          "INSERT INTO "
              + STAIRWAY_INSTANCE_TABLE
              + " (stairway_id, stairway_name)"
              + " VALUES (:stairwayId, :stairwayName)";
      try (NamedParameterPreparedStatement statement =
          new NamedParameterPreparedStatement(connection, sqlStairwayInstanceCreate)) {
        stairwayId = ShortUUID.get();

        statement.setString("stairwayName", stairwayName);
        statement.setString("stairwayId", stairwayId);
        statement.getPreparedStatement().executeUpdate();

        commitTransaction(connection);
        return stairwayId;
      }
    } catch (SQLException ex) {
      throw new DatabaseSetupException("Stairway instance find/create failed", ex);
    }
  }

  String lookupStairwayInstanceId(String stairwayName) throws DatabaseOperationException {
    try (Connection connection = dataSource.getConnection()) {
      startReadOnlyTransaction(connection);

      String stairwayId = lookupStairwayInstanceQuery(connection, stairwayName);

      commitTransaction(connection);
      return stairwayId;
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Stairway instance lookup failed", ex);
    }
  }

  private String lookupStairwayInstanceQuery(Connection connection, String stairwayName)
      throws SQLException, DatabaseOperationException {
    final String sqlStairwayInstance =
        "SELECT stairway_id"
            + " FROM "
            + STAIRWAY_INSTANCE_TABLE
            + " WHERE stairway_name = :stairwayName";

    try (NamedParameterPreparedStatement instanceStatement =
        new NamedParameterPreparedStatement(connection, sqlStairwayInstance)) {

      instanceStatement.setString("stairwayName", stairwayName);

      try (ResultSet rs = instanceStatement.getPreparedStatement().executeQuery()) {
        List<String> stairwayIdList = new ArrayList<>();
        while (rs.next()) {
          stairwayIdList.add(rs.getString("stairway_id"));
        }

        if (stairwayIdList.size() == 0) {
          return null;
        }
        if (stairwayIdList.size() > 1) {
          throw new DatabaseOperationException("Multiple stairways with the same name!");
        }
        return stairwayIdList.get(0);
      }
    }
  }

  /**
   * This call can be used in conjunction with a list of the currently active stairway instances to
   * drive recovery of orphaned flights.
   *
   * @return list of the stairway instances known to stairway
   * @throws DatabaseOperationException
   */
  List<String> getStairwayInstanceList() throws DatabaseOperationException {
    final String sql = "SELECT stairway_name FROM " + STAIRWAY_INSTANCE_TABLE;
    List<String> instanceList = new ArrayList<>();

    try (Connection connection = dataSource.getConnection()) {
      startReadOnlyTransaction(connection);

      try (NamedParameterPreparedStatement instanceStatement =
              new NamedParameterPreparedStatement(connection, sql);
          ResultSet rs = instanceStatement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          instanceList.add(rs.getString("stairway_name"));
        }
      }

      commitTransaction(connection);
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Stairway instance enumeration failed", ex);
    }
    return instanceList;
  }

  /** Record a new flight */
  void submit(FlightContext flightContext) throws DatabaseOperationException, InterruptedException {
    final String sqlInsertFlight =
        "INSERT INTO "
            + FLIGHT_TABLE
            + " (flightId, submit_time, class_name, status, stairway_id)"
            + "VALUES (:flightId, CURRENT_TIMESTAMP, :className, :status, :stairwayId)";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlInsertFlight)) {

      startTransaction(connection);
      statement.setString("flightId", flightContext.getFlightId());
      statement.setString("className", flightContext.getFlightClassName());
      statement.setString("status", flightContext.getFlightStatus().name());
      if (flightContext.getFlightStatus() == FlightStatus.READY) {
        // If we are submitting to ready, then we don't own the flight
        statement.setString("stairwayId", null);
      } else {
        statement.setString("stairwayId", flightContext.getStairway().getStairwayId());
      }
      statement.getPreparedStatement().executeUpdate();

      storeInputParameters(
          connection, flightContext.getFlightId(), flightContext.getInputParameters());

      commitTransaction(connection);
    } catch (SQLException ex) {
      handleSqlException("Failed to create database tables", ex, flightContext);
    }
  }

  /** Record the flight state right after a step Mark if we are re-running the step */
  void step(FlightContext flightContext) throws DatabaseOperationException, InterruptedException {
    final String sqlInsertFlightLog =
        "INSERT INTO "
            + FLIGHT_LOG_TABLE
            + "(flightid, log_time, working_parameters, step_index, rerun, direction,"
            + " succeeded, serialized_exception, status)"
            + " VALUES (:flightId, CURRENT_TIMESTAMP, :workingMap, :stepIndex, :rerun, :direction,"
            + " :succeeded, :serializedException, :status)";

    String serializedException =
        exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlInsertFlightLog)) {
      startTransaction(connection);
      statement.setString("flightId", flightContext.getFlightId());
      statement.setString("workingMap", flightContext.getWorkingMap().toJson());
      statement.setInt("stepIndex", flightContext.getStepIndex());
      statement.setBoolean("rerun", flightContext.isRerun());
      statement.setString("direction", flightContext.getDirection().name());
      statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
      statement.setString("serializedException", serializedException);
      statement.setString("status", flightContext.getFlightStatus().name());
      statement.getPreparedStatement().executeUpdate();
      commitTransaction(connection);
    } catch (SQLException ex) {
      handleSqlException("Failed to log step", ex, flightContext);
    }
  }

  /**
   * Record the exiting of the flight. It may be exiting because
   *
   * <ul>
   *   <li>it is done
   *   <li>it asked for a long wait
   *   <li>stairway is shutting down
   * </ul>
   *
   * @param flightContext context object for the flight
   * @throws DatabaseOperationException on database errors
   * @throws FlightException on invalid flight state
   */
  void exit(FlightContext flightContext)
      throws DatabaseOperationException, FlightException, InterruptedException {
    switch (flightContext.getFlightStatus()) {
      case SUCCESS:
      case ERROR:
      case FATAL:
        complete(flightContext);
        break;

      case WAITING:
      case READY:
        disown(flightContext);
        break;

      case QUEUED:
        queued(flightContext.getFlightId());

      case RUNNING:
      default:
        // invalid states
        throw new FlightException("Attempt to exit a flight in the running state");
    }
  }

  private static final int SERIALIZATION_RETRIES = 20;
  private static final int WAIT_MIN = 250;
  private static final int WAIT_MAX = 1000;

  private void retryWait(String logTag, String flightId) throws InterruptedException {
    int sleepMS = ThreadLocalRandom.current().nextInt(WAIT_MIN, WAIT_MAX);
    TimeUnit.MILLISECONDS.sleep(sleepMS);
    logger.info(logTag + " - retrying for id: " + flightId);
  }

  /**
   * Record that a flight has been put in the work queue. This is best effort to minimize the
   * chances of getting multiple entries for the same flight in the queue. However, by the time we
   * do this, another Stairway instance might already have pulled it from the queue and run it.
   *
   * @param flightId identifier for this flight
   * @throws DatabaseOperationException on database errors
   */
  void queued(String flightId) throws DatabaseOperationException, InterruptedException {
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = :status"
            + " WHERE stairway_id IS NULL AND flightid = :flightId AND status = 'READY'";
    updateFlightState("queued", sqlUpdateFlight, flightId, "QUEUED");
  }

  /**
   * Record that a flight is paused and no longer owned by this Stairway instance
   *
   * @param flightContext context object for the flight
   * @throws DatabaseOperationException on database errors
   */
  private void disown(FlightContext flightContext)
      throws DatabaseOperationException, InterruptedException {
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = :status,"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightId AND status = 'RUNNING'";
    updateFlightState(
        "disown",
        sqlUpdateFlight,
        flightContext.getFlightId(),
        flightContext.getFlightStatus().name());
  }

  private void updateFlightState(String comment, String sql, String flightId, String status)
      throws DatabaseOperationException, InterruptedException {

    for (int retry = 0; retry < SERIALIZATION_RETRIES; retry++) {
      try (Connection connection = dataSource.getConnection();
          NamedParameterPreparedStatement statement =
              new NamedParameterPreparedStatement(connection, sql)) {

        startTransaction(connection);
        statement.setString("status", status);
        statement.setString("flightId", flightId);
        statement.getPreparedStatement().executeUpdate();
        commitTransaction(connection);
        break;
      } catch (SQLException ex) {
        if (!retrySqlException(ex)) {
          throw new DatabaseOperationException(
              "Failed to update status to " + status + " for flight: " + flightId, ex);
        }
      }
      retryWait(comment, flightId);
    }
  }

  /**
   * This method is used during recovery of an obsolete stairway instance. It "disowns" all of the
   * flights that were owned by that stairway and puts them in the READY state. It then removes the
   * obsolete stairway instance from the stairway_instance table.
   *
   * @param stairwayId the id of the, presumably deleted, stairway instance
   * @return list of the flight ids that we disowned.
   * @throws DatabaseOperationException on database error
   * @throws InterruptedException interruption of the retry loop
   */
  void disownRecovery(String stairwayId) throws DatabaseOperationException, InterruptedException {
    final String sql =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = 'READY', stairway_id = NULL"
            + " WHERE stairway_id = :stairwayId AND status = 'RUNNING'";

    final String sqlDelete =
        "DELETE FROM " + STAIRWAY_INSTANCE_TABLE + " WHERE stairway_id = :stairwayId";

    for (int retry = 0; retry < SERIALIZATION_RETRIES; retry++) {
      try (Connection connection = dataSource.getConnection();
          NamedParameterPreparedStatement statement =
              new NamedParameterPreparedStatement(connection, sql);
          NamedParameterPreparedStatement deleteStatement =
              new NamedParameterPreparedStatement(connection, sqlDelete)) {

        startTransaction(connection);
        statement.setString("stairwayId", stairwayId);
        int disownCount = statement.getPreparedStatement().executeUpdate();
        logger.info("Disowned " + disownCount + " flights for stairway: " + stairwayId);

        deleteStatement.setString("stairwayId", stairwayId);
        deleteStatement.getPreparedStatement().executeUpdate();

        commitTransaction(connection);
        break;
      } catch (SQLException ex) {
        if (!retrySqlException(ex)) {
          throw new DatabaseOperationException(
              "Failed to disown flights owned by stairway instance: " + stairwayId, ex);
        }
      }
      retryWait("disownRecovery", stairwayId);
    }
  }

  /**
   * Build a collection of flight ids for all flights in the READY state. This is used as part of
   * recovery. We want to resubmit flights that are in the ready state.
   *
   * @return list of flight ids
   * @throws DatabaseOperationException error on database operations
   * @throws InterruptedException thread we are on is shutting down
   */
  List<String> getReadyFlights() throws DatabaseOperationException, InterruptedException {
    final String sql =
        "SELECT flightid FROM " + FLIGHT_TABLE + " WHERE stairway_id IS NULL AND status = 'READY'";

    List<String> flightList = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {

      startTransaction(connection);

      try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          flightList.add(rs.getString("flightid"));
        }
      }
      logger.info("Found ready flights: " + flightList.size());

      commitTransaction(connection);
    } catch (SQLException ex) {
      handleSqlException("Failed to get ready flight list", ex);
    }

    return flightList;
  }

  /**
   * Record completion of a flight. This may be because the flight is all done, or because the
   * flight is ending due to a yield or a shutdown of this stairway instance.
   *
   * <p>If the flight is all done, we remove the detailed step data from the log table.
   */
  private void complete(FlightContext flightContext)
      throws DatabaseOperationException, InterruptedException {
    // Make the update idempotent; that is, only do it if the status is RUNNING
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET completed_time = CURRENT_TIMESTAMP,"
            + " output_parameters = :outputParameters,"
            + " status = :status,"
            + " serialized_exception = :serializedException,"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightId AND status = 'RUNNING'";

    // The delete is harmless if it has been done before. We just won't find anything.
    final String sqlDeleteFlightLog =
        "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightId";

    String serializedException =
        exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

    for (int retry = 0; retry < SERIALIZATION_RETRIES; retry++) {
      try (Connection connection = dataSource.getConnection();
          NamedParameterPreparedStatement statement =
              new NamedParameterPreparedStatement(connection, sqlUpdateFlight);
          NamedParameterPreparedStatement deleteStatement =
              new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

        startTransaction(connection);

        statement.setString("outputParameters", flightContext.getWorkingMap().toJson());
        statement.setString("status", flightContext.getFlightStatus().name());
        statement.setString("serializedException", serializedException);
        statement.setString("flightId", flightContext.getFlightId());
        statement.getPreparedStatement().executeUpdate();

        if (!keepFlightLog) {
          deleteStatement.setString("flightId", flightContext.getFlightId());
          deleteStatement.getPreparedStatement().executeUpdate();
        }

        commitTransaction(connection);
        break;
      } catch (SQLException ex) {
        if (!retrySqlException(ex)) {
          throw new DatabaseOperationException(
              "Failed to complete flight: " + flightContext.getFlightId(), ex);
        }
      }
      retryWait("complete", flightContext.getFlightId());
    }
  }

  /**
   * Remove all record of this flight from the database
   *
   * @param flightId flight to remove
   * @throws DatabaseOperationException on any database error
   */
  void delete(String flightId) throws DatabaseOperationException, InterruptedException {
    final String sqlDeleteFlightLog =
        "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightId";
    final String sqlDeleteFlight = "DELETE FROM " + FLIGHT_TABLE + " WHERE flightid = :flightId";
    final String sqlDeleteFlightInput =
        "DELETE FROM " + FLIGHT_INPUT_TABLE + " WHERE flightid = :flightId";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement deleteFlightStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlight);
        NamedParameterPreparedStatement deleteInputStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightInput);
        NamedParameterPreparedStatement deleteLogStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

      startTransaction(connection);

      deleteFlightStatement.setString("flightId", flightId);
      deleteFlightStatement.getPreparedStatement().executeUpdate();

      deleteInputStatement.setString("flightId", flightId);
      deleteInputStatement.getPreparedStatement().executeUpdate();

      deleteLogStatement.setString("flightId", flightId);
      deleteLogStatement.getPreparedStatement().executeUpdate();

      commitTransaction(connection);

    } catch (SQLException ex) {
      handleSqlException("Failed to delete flight", ex, UNKNOWN, flightId);
    }
  }

  /*
   * Find one unowned flight, claim ownership, and return its flight context
   * Returns null if the flight does not exist or is not in the right state to be resumed.
   */
  FlightContext resume(String stairwayId, String flightId)
      throws DatabaseOperationException, InterruptedException {
    final String sqlUnownedFlight =
        "SELECT class_name "
            + " FROM "
            + FLIGHT_TABLE
            + " WHERE (status = 'WAITING' OR status = 'READY' OR status = 'QUEUED')"
            + " AND stairway_id IS NULL AND flightid = :flightId";

    final String sqlTakeOwnership =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = 'RUNNING',"
            + " stairway_id = :stairwayId"
            + " WHERE flightid = :flightId";

    for (int retry = 0; retry < SERIALIZATION_RETRIES; retry++) {
      try (Connection connection = dataSource.getConnection();
          NamedParameterPreparedStatement unownedFlightStatement =
              new NamedParameterPreparedStatement(connection, sqlUnownedFlight);
          NamedParameterPreparedStatement takeOwnershipStatement =
              new NamedParameterPreparedStatement(connection, sqlTakeOwnership)) {

        startTransaction(connection);

        unownedFlightStatement.setString("flightId", flightId);
        FlightContext flightContext = null;
        try (ResultSet rs = unownedFlightStatement.getPreparedStatement().executeQuery()) {
          if (rs.next()) {
            List<FlightInput> inputList = retrieveInputParameters(connection, flightId);
            FlightMap inputParameters = new FlightMap(inputList);
            flightContext = new FlightContext(inputParameters, rs.getString("class_name"));
            flightContext.setFlightId(flightId);

            fillFlightContexts(connection, Collections.singletonList(flightContext));
          }
        }

        if (flightContext != null) {
          logger.info("Stairway " + stairwayId + " taking ownership of flight " + flightId);
          takeOwnershipStatement.setString("flightId", flightId);
          takeOwnershipStatement.setString("stairwayId", stairwayId);
          takeOwnershipStatement.getPreparedStatement().executeUpdate();
        }

        commitTransaction(connection);
        return flightContext;
      } catch (SQLException ex) {
        if (!retrySqlException(ex)) {
          handleSqlException("Failed to get flight", ex);
          return null;
        }
      }
      retryWait("resume", flightId);
    }
    return null;
  }

  /**
   * Loop through the flight context list making a query for each flight to fill in the
   * FlightContext. This may not be the most efficient algorithm. My reasoning is that the code is
   * more obvious to understand and this is not a performance-critical part of the processing.
   *
   * @param connection database connection to use
   * @param flightContextList list of flight context objects to fill in
   * @throws DatabaseOperationException on database errors
   */
  private void fillFlightContexts(Connection connection, List<FlightContext> flightContextList)
      throws DatabaseOperationException, InterruptedException {

    final String sqlLastFlightLog =
        "SELECT working_parameters, step_index, direction, rerun,"
            + " succeeded, serialized_exception, status"
            + " FROM "
            + FLIGHT_LOG_TABLE
            + " WHERE flightid = :flightId AND log_time = "
            + " (SELECT MAX(log_time) FROM "
            + FLIGHT_LOG_TABLE
            + " WHERE flightid = :flightId2)";

    try (NamedParameterPreparedStatement lastFlightLogStatement =
        new NamedParameterPreparedStatement(connection, sqlLastFlightLog)) {

      for (FlightContext flightContext : flightContextList) {
        lastFlightLogStatement.setString("flightId", flightContext.getFlightId());
        lastFlightLogStatement.setString("flightId2", flightContext.getFlightId());

        try (ResultSet rsflight = lastFlightLogStatement.getPreparedStatement().executeQuery()) {
          // There may not be any log entries for a given flight. That happens if we fail after
          // submit and before the first step. The defaults for flight context are correct for that
          // case, so there is nothing left to do here.
          if (rsflight.next()) {
            StepResult stepResult;
            if (rsflight.getBoolean("succeeded")) {
              stepResult = StepResult.getStepResultSuccess();
            } else {
              stepResult =
                  new StepResult(
                      exceptionSerializer.deserialize(rsflight.getString("serialized_exception")));
            }

            flightContext.getWorkingMap().fromJson(rsflight.getString("working_parameters"));

            flightContext.setRerun(rsflight.getBoolean("rerun"));
            flightContext.setDirection(Direction.valueOf(rsflight.getString("direction")));
            flightContext.setResult(stepResult);
            flightContext.setFlightStatus(FlightStatus.valueOf(rsflight.getString("status")));
            flightContext.setStepIndex(rsflight.getInt("step_index"));
          }
        }
      }
    } catch (SQLException ex) {
      handleSqlException("Failed to get flight log data", ex);
    }
  }

  /**
   * Return flight state for a single flight
   *
   * @param flightId flight to get
   * @return FlightState for the flight
   */
  FlightState getFlightState(String flightId)
      throws DatabaseOperationException, InterruptedException {
    final String sqlOneFlight =
        "SELECT stairway_id, flightid, submit_time, "
            + " completed_time, output_parameters, status, serialized_exception"
            + " FROM "
            + FLIGHT_TABLE
            + " WHERE flightid = :flightId";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement oneFlightStatement =
            new NamedParameterPreparedStatement(connection, sqlOneFlight)) {

      startReadOnlyTransaction(connection);
      oneFlightStatement.setString("flightId", flightId);

      try (ResultSet rs = oneFlightStatement.getPreparedStatement().executeQuery()) {
        List<FlightState> flightStateList = makeFlightStateList(connection, rs);
        if (flightStateList.size() == 0) {
          throw new FlightNotFoundException("Flight not found: " + flightId);
        }
        if (flightStateList.size() > 1) {
          throw new DatabaseOperationException("Multiple flights with the same id?!");
        }
        commitTransaction(connection);
        return flightStateList.get(0);
      }

    } catch (SQLException ex) {
      handleSqlException("Failed to get flight", ex, UNKNOWN, flightId);
      return null;
    }
  }

  /**
   * Get a list of flights and their states. The list may be restricted by providing one or more
   * filters. The filters are logically ANDed together.
   *
   * <p>For performance, there are three forms of the query.
   *
   * <p><bold>Form 1: no input parameter filters</bold>
   *
   * <p>When there are no input parameter filters, then we can do a single table query on the flight
   * table like: {@code SELECT flight.* FROM flight WHERE flight-filters ORDER BY } The WHERE and
   * flight-filters are omitted if there are none to apply.
   *
   * <p><bold>Form 2: one input parameter</bold>
   *
   * <p>When there is one input filter restriction, we can do a simple join with a restricting join
   * against the input table like:
   *
   * <pre>{@code
   * SELECT flight.*
   * FROM flight JOIN flight_input
   *   ON flight.flightId = flight_input.flightId
   * WHERE flight-filters
   *   AND flight_input.key = 'key' AND flight_input.value = 'json-of-object'
   * }</pre>
   *
   * <bold>Form 3: more than one input parameter</bold>
   *
   * <p>This one gets complicated. We do a subquery that filters by the OR of the input filters and
   * groups by the COUNT of the matches. Only flights where we have inputs that qualify by the
   * filter will have the right count of matches. The query form is like:
   *
   * <pre>{@code
   * SELECT flight.*
   * FROM flight
   * JOIN (SELECT flightId, COUNT(*) AS matchCount
   *       FROM flight_input
   *       WHERE (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *          OR (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *          OR (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *       GROUP BY flightId) INPUT
   * ON flight.flightId = INPUT.flightId
   * WHERE flight-filters
   *   AND INPUT.matchCount = 3
   * }</pre>
   *
   * In all cases, the result is sorted and paged like this: (@code ORDER BY submit_time LIMIT
   * :limit OFFSET :offset}
   *
   * @param offset offset into the result set to start returning
   * @param limit max number of results to return
   * @param inFilter filters to apply to the flights
   * @return list of FlightState objects for the filtered, paged flights
   * @throws DatabaseOperationException on all database issues
   */
  List<FlightState> getFlights(int offset, int limit, FlightFilter inFilter)
      throws DatabaseOperationException, InterruptedException {

    // Make an empty filter if one is not provided
    FlightFilter filter = (inFilter != null) ? inFilter : new FlightFilter();
    String sql = filter.makeSql();

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement flightRangeStatement =
            new NamedParameterPreparedStatement(connection, sql)) {
      startReadOnlyTransaction(connection);

      filter.storePredicateValues(flightRangeStatement);

      flightRangeStatement.setInt("limit", limit);
      flightRangeStatement.setInt("offset", offset);

      List<FlightState> flightStateList;
      try (ResultSet rs = flightRangeStatement.getPreparedStatement().executeQuery()) {
        flightStateList = makeFlightStateList(connection, rs);
      }

      connection.commit();
      return flightStateList;
    } catch (SQLException ex) {
      handleSqlException("Failed to get flights", ex);
      return null;
    } catch (FlightFilterException ex) {
      throw new DatabaseOperationException("Failed to get flights", ex);
    }
  }

  private List<FlightState> makeFlightStateList(Connection connection, ResultSet rs)
      throws SQLException, DatabaseOperationException, InterruptedException {
    List<FlightState> flightStateList = new ArrayList<>();

    while (rs.next()) {
      String flightId = rs.getString("flightid");
      FlightState flightState = new FlightState();

      // Flight data that is always present
      flightState.setFlightId(flightId);
      flightState.setFlightStatus(FlightStatus.valueOf(rs.getString("status")));
      flightState.setSubmitted(rs.getTimestamp("submit_time").toInstant());
      flightState.setStairwayId(rs.getString("stairway_id"));
      List<FlightInput> flightInput = retrieveInputParameters(connection, flightId);
      flightState.setInputParameters(new FlightMap(flightInput));

      // If the flight is in one of the complete states, then we retrieve the completion data
      if (flightState.getFlightStatus() == FlightStatus.SUCCESS
          || flightState.getFlightStatus() == FlightStatus.ERROR
          || flightState.getFlightStatus() == FlightStatus.FATAL) {
        flightState.setCompleted(rs.getTimestamp("completed_time").toInstant());
        flightState.setException(
            exceptionSerializer.deserialize(rs.getString("serialized_exception")));
        String outputParamsJson = rs.getString("output_parameters");
        if (outputParamsJson != null) {
          FlightMap outputParameters = new FlightMap();
          outputParameters.fromJson(outputParamsJson);
          flightState.setResultMap(outputParameters);
        }
      }

      flightStateList.add(flightState);
    }

    return flightStateList;
  }

  private void storeInputParameters(
      Connection connection, String flightId, FlightMap inputParameters)
      throws DatabaseOperationException, InterruptedException {
    List<FlightInput> inputList = inputParameters.makeFlightInputList();

    final String sqlInsertInput =
        "INSERT INTO "
            + FLIGHT_INPUT_TABLE
            + " (flightId, key, value) VALUES (:flightId, :key, :value)";

    try (NamedParameterPreparedStatement statement =
        new NamedParameterPreparedStatement(connection, sqlInsertInput)) {

      statement.setString("flightId", flightId);

      for (FlightInput input : inputList) {
        statement.setString("key", input.getKey());
        statement.setString("value", input.getValue());
        statement.getPreparedStatement().executeUpdate();
      }

    } catch (SQLException ex) {
      handleSqlException("Failed to insert input", ex, UNKNOWN, flightId);
    }
  }

  private List<FlightInput> retrieveInputParameters(Connection connection, String flightId)
      throws DatabaseOperationException, InterruptedException {
    final String sqlSelectInput =
        "SELECT flightId, key, value FROM " + FLIGHT_INPUT_TABLE + " WHERE flightId = :flightId";

    List<FlightInput> inputList = new ArrayList<>();

    try (NamedParameterPreparedStatement statement =
        new NamedParameterPreparedStatement(connection, sqlSelectInput)) {
      statement.setString("flightId", flightId);

      try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          FlightInput input = new FlightInput(rs.getString("key"), rs.getString("value"));
          inputList.add(input);
        }
      }
    } catch (SQLException ex) {
      handleSqlException("Failed to select input", ex, UNKNOWN, flightId);
    }

    return inputList;
  }

  private void startTransaction(Connection connection) throws SQLException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    connection.setReadOnly(false);
  }

  private void startReadOnlyTransaction(Connection connection) throws SQLException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    connection.setReadOnly(true);
  }

  private void commitTransaction(Connection connection) throws SQLException {
    connection.commit();
  }

  private void handleSqlException(String message, SQLException ex)
      throws InterruptedException, DatabaseOperationException {
    handleSqlException(message, ex, UNKNOWN, UNKNOWN);
  }

  private void handleSqlException(String message, SQLException ex, FlightContext context)
      throws InterruptedException, DatabaseOperationException {
    handleSqlException(message, ex, context.getStairway().getStairwayId(), context.getFlightId());
  }

  private void handleSqlException(
      String message, SQLException ex, String stairwayId, String flightId)
      throws InterruptedException, DatabaseOperationException {

    if (ex.getCause() instanceof InterruptedException) {
      logger.warn(
          "Operation interrupted - stairwayId: "
              + stairwayId
              + " flightId: "
              + flightId
              + "message: "
              + message,
          ex);
      String causeText = (ex.getCause() == null) ? "unknown cause" : ex.getCause().toString();
      throw new InterruptedException(causeText);
    }
    logger.warn(
        "Operation error - stairwayId: "
            + stairwayId
            + " flightId: "
            + flightId
            + "message: "
            + message,
        ex);
    throw new DatabaseOperationException(message, ex);
  }

  private static final String PSQL_SERIALIZATION_FAILURE = "40001";
  private static final String PSQL_DEADLOCK_DETECTED = "40P01";
  private static final String PSQL_CONNECTION_ISSUE_PREFIX = "08";
  private static final String PSQL_RESOURCE_ISSUE_PREFIX = "53";

  private boolean retrySqlException(SQLException ex) {
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
}
