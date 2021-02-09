package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightFilterException;
import bio.terra.stairway.exception.FlightNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
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

import static bio.terra.stairway.DbUtils.commitTransaction;
import static bio.terra.stairway.DbUtils.startReadOnlyTransaction;
import static bio.terra.stairway.DbUtils.startTransaction;

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

  private static final String UNKNOWN = "<unknown>";

  private final DataSource dataSource;
  private final StairwayInstanceDao stairwayInstanceDao;
  private final ExceptionSerializer exceptionSerializer;
  private final boolean keepFlightLog;
  private final HookWrapper hookWrapper;

  FlightDao(
      DataSource dataSource,
      StairwayInstanceDao stairwayInstanceDao,
      ExceptionSerializer exceptionSerializer,
      HookWrapper hookWrapper,
      boolean keepFlightLog) {
    this.dataSource = dataSource;
    this.stairwayInstanceDao = stairwayInstanceDao;
    this.exceptionSerializer = exceptionSerializer;
    this.hookWrapper = hookWrapper;
    this.keepFlightLog = keepFlightLog;
  }

  /**
   * Record a new flight
   *
   * @param flightContext description of the flight
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void submit(FlightContext flightContext) throws DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.submit", () -> submitInner(flightContext));
  }

  private void submitInner(FlightContext flightContext)
      throws SQLException, DatabaseOperationException, InterruptedException {
    final String sqlInsertFlight =
        "INSERT INTO "
            + FLIGHT_TABLE
            + " (flightId, submit_time, class_name, status, stairway_id, debug_info)"
            + "VALUES (:flightId, CURRENT_TIMESTAMP, :className, :status, :stairwayId, :debugInfo)";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlInsertFlight)) {

      startTransaction(connection);
      statement.setString("flightId", flightContext.getFlightId());
      statement.setString("className", flightContext.getFlightClassName());
      statement.setString("status", flightContext.getFlightStatus().name());
      if (flightContext.getFlightStatus() == FlightStatus.READY
          || flightContext.getFlightStatus() == FlightStatus.READY_TO_RESTART) {
        // If we are submitting to ready, then we don't own the flight
        statement.setString("stairwayId", null);
      } else {
        statement.setString("stairwayId", flightContext.getStairway().getStairwayId());
      }
      if (flightContext.getDebugInfo() != null) {
        statement.setString("debugInfo", flightContext.getDebugInfo().toString());
      } else {
        statement.setString("debugInfo", "{}");
      }
      statement.getPreparedStatement().executeUpdate();

      storeInputParameters(
          connection, flightContext.getFlightId(), flightContext.getInputParameters());

      commitTransaction(connection);
      hookWrapper.stateTransition(flightContext);
    } catch (SQLException ex) {
      // SQL state 23505 indicates a unique key violation, which in this case indicates a duplicate
      // flightId. See https://www.postgresql.org/docs/10/errcodes-appendix.html for postgres
      // error codes.
      if (ex.getSQLState().equals("23505")) {
        throw new DuplicateFlightIdException(
            "Duplicate flightID " + flightContext.getFlightId(), ex);
      }
      throw ex;
    }
  }

  /**
   * Record the flight state right after a step
   *
   * @param flightContext description of the flight
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void step(FlightContext flightContext) throws DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.step", () -> stepInner(flightContext));
  }

  private void stepInner(FlightContext flightContext) throws SQLException {
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
      // TODO: I believe storing this is useless. The status always RUNNING
      statement.setString("status", flightContext.getFlightStatus().name());
      statement.getPreparedStatement().executeUpdate();
      commitTransaction(connection);
    }
  }

  /**
   * Record the exiting of the flight. In this case, we interpret the flight status as the target
   * status for the flight.
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

      case READY_TO_RESTART:
      case WAITING:
      case READY:
        disown(flightContext);
        break;

      case QUEUED:
        queued(flightContext);
        break;

      case RUNNING:
      default:
        // invalid states
        throw new FlightException("Attempt to exit a flight in the running state");
    }
  }

  /**
   * Record that a flight has been put in the work queue. This is best effort to minimize the
   * chances of getting multiple entries for the same flight in the queue. However, by the time we
   * do this, another Stairway instance might already have pulled it from the queue and run it.
   *
   * @param flightContext context for this flight
   * @throws DatabaseOperationException on database errors
   * @throws InterruptedException thread shutdown
   */
  void queued(FlightContext flightContext) throws DatabaseOperationException, InterruptedException {
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = :status"
            + " WHERE stairway_id IS NULL AND flightid = :flightId AND status = 'READY'";
    flightContext.setFlightStatus(FlightStatus.QUEUED);
    DbRetry.retryVoid("flight.queued", () -> updateFlightState(sqlUpdateFlight, flightContext));
  }

  /**
   * Record that a flight is paused and no longer owned by this Stairway instance
   *
   * @param flightContext context object for the flight
   * @throws DatabaseOperationException on database errors
   * @throws InterruptedException thread shutdown
   */
  private void disown(FlightContext flightContext)
      throws DatabaseOperationException, InterruptedException {
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = :status,"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightId AND status = 'RUNNING'";
    DbRetry.retryVoid("flight.disown", () -> updateFlightState(sqlUpdateFlight, flightContext));
  }

  private void updateFlightState(String sql, FlightContext flightContext) throws SQLException {

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {

      startTransaction(connection);
      statement.setString("status", flightContext.getFlightStatus().name());
      statement.setString("flightId", flightContext.getFlightId());
      statement.getPreparedStatement().executeUpdate();
      commitTransaction(connection);
      hookWrapper.stateTransition(flightContext);
    }
  }

  /**
   * This method is used during recovery of an obsolete stairway instance. It "disowns" all of the
   * flights that were owned by that stairway and puts them in the READY state. It then removes the
   * obsolete stairway instance from the stairway_instance table.
   *
   * @param stairwayId the id of the, presumably deleted, stairway instance
   * @throws DatabaseOperationException on database error
   * @throws InterruptedException interruption of the retry loop
   */
  void disownRecovery(String stairwayId) throws DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.disownRecovery", () -> disownRecoveryInner(stairwayId));
  }

  private void disownRecoveryInner(String stairwayId)
      throws SQLException, DatabaseOperationException, InterruptedException {
    final String sqlGet =
        "SELECT flightid, class_name, debug_info, status FROM "
            + FLIGHT_TABLE
            + " WHERE stairway_id = :stairwayId AND status = 'RUNNING'";

    final String sqlUpdate =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = 'READY', stairway_id = NULL"
            + " WHERE stairway_id = :stairwayId AND status = 'RUNNING'";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement getStatement =
            new NamedParameterPreparedStatement(connection, sqlGet);
        NamedParameterPreparedStatement updateStatement =
            new NamedParameterPreparedStatement(connection, sqlUpdate)) {

      startTransaction(connection);
      getStatement.setString("stairwayId", stairwayId);
      List<FlightContext> flightList = new ArrayList<>();
      try (ResultSet rs = getStatement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          FlightContext flightContext = makeFlightContext(connection, rs.getString("flightid"), rs);
          flightContext.setFlightStatus(FlightStatus.READY);
          flightList.add(flightContext);
        }
      }

      if (!flightList.isEmpty()) {
        updateStatement.setString("stairwayId", stairwayId);
        int disownCount = updateStatement.getPreparedStatement().executeUpdate();
        logger.info("Disowned " + disownCount + " flights for stairway: " + stairwayId);
      }

      stairwayInstanceDao.delete(connection, stairwayId);

      commitTransaction(connection);

      // Call the state hook for each of the flights we found
      for (FlightContext flightContext : flightList) {
        hookWrapper.stateTransition(flightContext);
      }
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
    return DbRetry.retry("flight.getReadyFlights", this::getReadyFlightsInner);
  }

  private List<String> getReadyFlightsInner() throws SQLException {
    final String sql =
        "SELECT flightid FROM "
            + FLIGHT_TABLE
            + " WHERE stairway_id IS NULL AND "
            + "(status = 'READY' OR status = 'READY_TO_RESTART')";
    List<String> flightList = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {

      // My concern is that some snapshot transaction would be reading
      // out of date data, because there would be other transactions writing to
      // the table. Making this a serialized transaction makes sure that the read
      // transaction is ordered with the write transactions.
      startTransaction(connection);

      try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          flightList.add(rs.getString("flightid"));
        }
      }
      logger.info("Found ready flights: " + flightList.size());

      commitTransaction(connection);
    }
    return flightList;
  }

  /**
   * Record completion of a flight. This may be because the flight is all done, or because the
   * flight is ending due to a yield or a shutdown of this stairway instance.
   *
   * <p>If the flight is all done, we remove the detailed step data from the log table.
   *
   * @param flightContext flight description
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  private void complete(FlightContext flightContext)
      throws DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.complete", () -> completeInner(flightContext));
  }

  private void completeInner(FlightContext flightContext) throws SQLException {
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
      hookWrapper.stateTransition(flightContext);
    }
  }

  /**
   * Remove all record of this flight from the database
   *
   * @param flightId flight to remove
   * @throws DatabaseOperationException on any database error
   * @throws InterruptedException thread shutdown
   */
  void delete(String flightId) throws DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.delete", () -> deleteInner(flightId));
  }

  private void deleteInner(String flightId) throws SQLException {
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
    }
  }

  /**
   * Find one unowned flight, claim ownership, and return its flight context
   *
   * @param stairwayId identifier of stairway to own the resumed flight
   * @param flightId identifier of flight to resume
   * @return resumed flight; null if the flight does not exist or is not in the right state to be
   *     resumed
   * @throws DatabaseOperationException databases failure
   * @throws InterruptedException thread shutdown
   */
  FlightContext resume(String stairwayId, String flightId)
      throws DatabaseOperationException, InterruptedException {
    return DbRetry.retry("flight.resume", () -> resumeInner(stairwayId, flightId));
  }

  private FlightContext resumeInner(String stairwayId, String flightId)
      throws SQLException, DatabaseOperationException, InterruptedException {
    final String sqlUnownedFlight =
        "SELECT class_name, debug_info, status "
            + " FROM "
            + FLIGHT_TABLE
            + " WHERE (status = 'WAITING' OR status = 'READY' OR status = 'QUEUED' OR status = 'READY_TO_RESTART')"
            + " AND stairway_id IS NULL AND flightid = :flightId";

    final String sqlTakeOwnership =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = 'RUNNING',"
            + " stairway_id = :stairwayId"
            + " WHERE flightid = :flightId";

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
          flightContext = makeFlightContext(connection, flightId, rs);
          flightContext.setFlightStatus(FlightStatus.RUNNING);
        }
      }

      if (flightContext != null) {
        logger.info("Stairway " + stairwayId + " taking ownership of flight " + flightId);
        takeOwnershipStatement.setString("flightId", flightId);
        takeOwnershipStatement.setString("stairwayId", stairwayId);
        takeOwnershipStatement.getPreparedStatement().executeUpdate();
      }

      commitTransaction(connection);

      if (flightContext != null) {
        hookWrapper.stateTransition(flightContext);
      }
      return flightContext;
    }
  }

  /**
   * Given a flightId build the flight context from the database
   *
   * @param flightId identifier of the flight
   * @return constructed flight context
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  FlightContext makeFlightContextById(String flightId)
      throws DatabaseOperationException, InterruptedException {
    return DbRetry.retry(
        "flight.makeFlightcontextById", () -> makeFlightContextByIdInner(flightId));
  }

  private FlightContext makeFlightContextByIdInner(String flightId)
      throws SQLException, DatabaseOperationException, InterruptedException {
    final String sqlGetFlight =
        "SELECT class_name, debug_info, status "
            + " FROM "
            + FLIGHT_TABLE
            + " WHERE flightid = :flightId";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement getFlightStatement =
            new NamedParameterPreparedStatement(connection, sqlGetFlight)) {

      startTransaction(connection);

      getFlightStatement.setString("flightId", flightId);
      FlightContext flightContext = null;
      try (ResultSet rs = getFlightStatement.getPreparedStatement().executeQuery()) {
        if (rs.next()) {
          flightContext = makeFlightContext(connection, flightId, rs);
        }
      }
      commitTransaction(connection);
      return flightContext;
    }
  }

  // The results set must be positioned at a row from the flight table,
  // and the select list must include debug_info, class_name, and status.
  private FlightContext makeFlightContext(Connection connection, String flightId, ResultSet rs)
      throws InterruptedException, DatabaseOperationException, SQLException {
    List<FlightInput> inputList = retrieveInputParameters(connection, flightId);
    FlightMap inputParameters = new FlightMap(inputList);
    FlightDebugInfo debugInfo = null;
    try {
      debugInfo =
          rs.getString("debug_info") == null
              ? null
              : FlightDebugInfo.getObjectMapper()
                  .readValue(rs.getString("debug_info"), FlightDebugInfo.class);
    } catch (JsonProcessingException e) {
      throw new DatabaseOperationException(e);
    }
    FlightContext flightContext =
        new FlightContext(inputParameters, rs.getString("class_name"), Collections.emptyList());
    flightContext.setDebugInfo(debugInfo);
    flightContext.setFlightId(flightId);

    fillFlightContexts(connection, Collections.singletonList(flightContext));
    flightContext.setFlightStatus(FlightStatus.valueOf(rs.getString("status")));
    return flightContext;
  }

  /**
   * Loop through the flight context list making a query for each flight to fill in the
   * FlightContext. This may not be the most efficient algorithm. My reasoning is that the code is
   * more obvious to understand and this is not a performance-critical part of the processing.
   *
   * @param connection database connection to use
   * @param flightContextList list of flight context objects to fill in
   * @throws SQLException on database errors
   */
  private void fillFlightContexts(Connection connection, List<FlightContext> flightContextList)
      throws SQLException {

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
    return DbRetry.retry("flight.getFlightState", () -> getFlightStateInner(flightId));
  }

  private FlightState getFlightStateInner(String flightId)
      throws SQLException, DatabaseOperationException {

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
   * @throws InterruptedException thread shutdown
   */
  List<FlightState> getFlights(int offset, int limit, FlightFilter inFilter)
      throws DatabaseOperationException, InterruptedException {

    // Make an empty filter if one is not provided
    FlightFilter filter = (inFilter != null) ? inFilter : new FlightFilter();

    return DbRetry.retry("flight.getFlights", () -> getFlightsInner(offset, limit, filter));
  }

  private List<FlightState> getFlightsInner(int offset, int limit, FlightFilter filter)
      throws SQLException, DatabaseOperationException {
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
    } catch (FlightFilterException ex) {
      throw new DatabaseOperationException("Failed to get flights", ex);
    }
  }

  private List<FlightState> makeFlightStateList(Connection connection, ResultSet rs)
      throws SQLException {
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
      Connection connection, String flightId, FlightMap inputParameters) throws SQLException {
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
    }
  }

  private List<FlightInput> retrieveInputParameters(Connection connection, String flightId)
      throws SQLException {
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
    }
    return inputList;
  }
}
