package bio.terra.stairway.impl;

import static bio.terra.stairway.impl.DbUtils.commitTransaction;
import static bio.terra.stairway.impl.DbUtils.startReadOnlyTransaction;
import static bio.terra.stairway.impl.DbUtils.startTransaction;

import bio.terra.stairway.Direction;
import bio.terra.stairway.ExceptionSerializer;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightEnumeration;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightInput;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.FlightFilterException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
class FlightDao {
  static final String FLIGHT_TABLE = "flight";
  static final String FLIGHT_LOG_TABLE = "flightlog";
  static final String FLIGHT_INPUT_TABLE = "flightinput";
  static final String FLIGHT_WORKING_TABLE = "flightworking";
  static final String FLIGHT_PERSISTED_TABLE = "flightpersisted";
  private static final Logger logger = LoggerFactory.getLogger(FlightDao.class);
  private static final String UNKNOWN = "<unknown>";

  private final DataSource dataSource;
  private final StairwayInstanceDao stairwayInstanceDao;
  private final ExceptionSerializer exceptionSerializer;
  private final HookWrapper hookWrapper;
  private final String stairwayId;

  FlightDao(
      DataSource dataSource,
      StairwayInstanceDao stairwayInstanceDao,
      ExceptionSerializer exceptionSerializer,
      HookWrapper hookWrapper,
      String stairwayId) {
    this.dataSource = dataSource;
    this.stairwayInstanceDao = stairwayInstanceDao;
    this.exceptionSerializer = exceptionSerializer;
    this.hookWrapper = hookWrapper;
    this.stairwayId = stairwayId;
  }

  /**
   * Create the record of a new flight
   *
   * @param flightContext description of the flight
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws DuplicateFlightIdException attempt to submit a flight with a duplicate id
   * @throws InterruptedException thread shutdown
   */
  void create(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, DuplicateFlightIdException,
          InterruptedException {
    DbRetry.retryVoid("flight.submit", () -> createInner(flightContext));
  }

  private void createInner(FlightContextImpl flightContext)
      throws SQLException, DuplicateFlightIdException {
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
        statement.setString("stairwayId", stairwayId);
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
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void step(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.step", () -> stepInner(flightContext));
  }

  private void stepInner(FlightContextImpl flightContext) throws SQLException {

    UUID logId = UUID.randomUUID();

    final String sqlInsertFlightLog =
        "INSERT INTO "
            + FLIGHT_LOG_TABLE
            + "(id, flightid, log_time, step_index, rerun, direction,"
            + " succeeded, serialized_exception, status)"
            + " VALUES (:logId, :flightId, CURRENT_TIMESTAMP, :stepIndex, :rerun, :direction,"
            + " :succeeded, :serializedException, :status)";

    String serializedException =
        exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlInsertFlightLog)) {
      startTransaction(connection);
      statement.setUuid("logId", logId);
      statement.setString("flightId", flightContext.getFlightId());
      statement.setInt("stepIndex", flightContext.getStepIndex());
      statement.setBoolean("rerun", flightContext.isRerun());
      statement.setString("direction", flightContext.getDirection().name());
      statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
      statement.setString("serializedException", serializedException);

      // TODO: I believe storing this is useless. The status always RUNNING
      statement.setString("status", flightContext.getFlightStatus().name());
      statement.getPreparedStatement().executeUpdate();

      storeWorkingParameters(connection, logId, flightContext.getWorkingMap());

      commitTransaction(connection);
    }
  }

  /**
   * Record the exiting of the flight. In this case, we interpret the flight status as the target
   * status for the flight.
   *
   * @param flightContext context object for the flight
   * @throws StairwayException other stairway exception
   * @throws StairwayExecutionException invalid flight state for exit
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void exit(FlightContextImpl flightContext)
      throws StairwayException, StairwayExecutionException, DatabaseOperationException,
          InterruptedException {
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
        throw new StairwayExecutionException("Attempt to exit a flight in the running state");
    }
  }

  /**
   * Record that a flight has been put in the work queue. This is best effort to minimize the
   * chances of getting multiple entries for the same flight in the queue. However, by the time we
   * do this, another Stairway instance might already have pulled it from the queue and run it.
   *
   * @param flightContext context for this flight
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void queued(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, InterruptedException {
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
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  private void disown(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET status = :status,"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightId AND status = 'RUNNING'";
    DbRetry.retryVoid("flight.disown", () -> updateFlightState(sqlUpdateFlight, flightContext));
  }

  private void updateFlightState(String sql, FlightContextImpl flightContext) throws SQLException {
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
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void disownRecovery(String stairwayId)
      throws StairwayException, DatabaseOperationException, InterruptedException {
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

      // We build a list of flight contexts. This is not necessary for disowning, but we need
      // to supply the flight context to the state transition hook, so we go to the trouble
      // of making the whole context.
      List<FlightContextImpl> flightList = new ArrayList<>();
      try (ResultSet rs = getStatement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          FlightContextImpl flightContext =
              makeFlightContext(connection, rs.getString("flightid"), rs);
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
      for (FlightContextImpl flightContext : flightList) {
        hookWrapper.stateTransition(flightContext);
      }
    }
  }

  /**
   * Build a collection of flight ids for all flights in the READY state. This is used as part of
   * recovery. We want to resubmit flights that are in the ready state.
   *
   * @return list of flight ids
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  List<String> getReadyFlights()
      throws StairwayException, DatabaseOperationException, InterruptedException {
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
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  private void complete(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.complete", () -> completeInner(flightContext));
  }

  private void completeInner(FlightContextImpl flightContext) throws SQLException {
    // Make the update idempotent; that is, only do it if the status is RUNNING
    final String sqlUpdateFlight =
        "UPDATE "
            + FLIGHT_TABLE
            + " SET completed_time = CURRENT_TIMESTAMP,"
            + " status = :status,"
            + " serialized_exception = :serializedException,"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightId AND status = 'RUNNING'";

    String serializedException =
        exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlUpdateFlight)) {

      startTransaction(connection);

      statement.setString("status", flightContext.getFlightStatus().name());
      statement.setString("serializedException", serializedException);
      statement.setString("flightId", flightContext.getFlightId());
      statement.getPreparedStatement().executeUpdate();

      commitTransaction(connection);
      hookWrapper.stateTransition(flightContext);
    }
  }

  /**
   * Remove all record of this flight from the database
   *
   * @param flightId flight to remove
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  void delete(String flightId)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid("flight.delete", () -> deleteInner(flightId));
  }

  private void deleteInner(String flightId) throws SQLException {
    final String sqlDeleteFlightLog =
        "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightId";
    final String sqlDeleteFlight = "DELETE FROM " + FLIGHT_TABLE + " WHERE flightid = :flightId";

    final String sqlDeleteFlightWorking =
        "DELETE FROM "
            + FLIGHT_WORKING_TABLE
            + " WHERE flightlog_id IN"
            + " (SELECT id FROM flightlog WHERE flightid = :flightId)";

    final String sqlDeleteFlightInput =
        "DELETE FROM " + FLIGHT_INPUT_TABLE + " WHERE flightid = :flightId";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement deleteFlightStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlight);
        NamedParameterPreparedStatement deleteInputStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightInput);
        NamedParameterPreparedStatement deleteWorkingStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightWorking);
        NamedParameterPreparedStatement deleteLogStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

      startTransaction(connection);

      deleteFlightStatement.setString("flightId", flightId);
      deleteFlightStatement.getPreparedStatement().executeUpdate();

      deleteInputStatement.setString("flightId", flightId);
      deleteInputStatement.getPreparedStatement().executeUpdate();

      deleteWorkingStatement.setString("flightId", flightId);
      deleteWorkingStatement.getPreparedStatement().executeUpdate();

      deleteLogStatement.setString("flightId", flightId);
      deleteLogStatement.getPreparedStatement().executeUpdate();

      commitTransaction(connection);
    }
  }

  /**
   * Remove completed flights from the database that are older than a specific time
   *
   * @param deleteOlderThan time before which flights can be removed
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   * @return count of deleted lights
   */
  int deleteCompletedFlights(Instant deleteOlderThan)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry(
        "flight.deleteCompletedFlights", () -> deleteCompletedFlightsInner(deleteOlderThan));
  }

  private int deleteCompletedFlightsInner(Instant deleteOlderThan) throws SQLException {
    final String sqlInClause =
        " WHERE flightid IN (SELECT flightid FROM "
            + FLIGHT_TABLE
            + " WHERE completed_time < :completed_time)";

    final String sqlDeleteFlightWorking =
        "DELETE FROM "
            + FLIGHT_WORKING_TABLE
            + " WHERE flightlog_id IN"
            + " (SELECT id FROM flightlog "
            + sqlInClause
            + ")";

    final String sqlDeleteFlightInput = "DELETE FROM " + FLIGHT_INPUT_TABLE + sqlInClause;

    final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + sqlInClause;

    final String sqlDeleteFlight =
        "DELETE FROM " + FLIGHT_TABLE + "  WHERE completed_time < :completed_time";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement deleteFlightStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlight);
        NamedParameterPreparedStatement deleteInputStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightInput);
        NamedParameterPreparedStatement deleteWorkingStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightWorking);
        NamedParameterPreparedStatement deleteLogStatement =
            new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

      startTransaction(connection);

      deleteWorkingStatement.setInstant("completed_time", deleteOlderThan);
      deleteWorkingStatement.getPreparedStatement().executeUpdate();

      deleteInputStatement.setInstant("completed_time", deleteOlderThan);
      deleteInputStatement.getPreparedStatement().executeUpdate();

      deleteLogStatement.setInstant("completed_time", deleteOlderThan);
      deleteLogStatement.getPreparedStatement().executeUpdate();

      deleteFlightStatement.setInstant("completed_time", deleteOlderThan);
      int count = deleteFlightStatement.getPreparedStatement().executeUpdate();

      commitTransaction(connection);
      return count;
    }
  }

  /**
   * Find one unowned flight, claim ownership, and return its flight context
   *
   * @param stairwayId identifier of stairway to own the resumed flight
   * @param flightId identifier of flight to resume
   * @return resumed flight; null if the flight does not exist or is not in the right state to be
   *     resumed
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  FlightContextImpl resume(String stairwayId, String flightId)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry("flight.resume", () -> resumeInner(stairwayId, flightId));
  }

  private FlightContextImpl resumeInner(String stairwayId, String flightId)
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
      FlightContextImpl flightContext = null;
      try (ResultSet rs = unownedFlightStatement.getPreparedStatement().executeQuery()) {
        if (rs.next()) {
          flightContext = makeFlightContext(connection, flightId, rs);
          // Set the flight status to RUNNING. That anticipates the execution of the
          // takeOwnership statement that will set the flight status in the database.
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

  void storePersistedStateMap(String flightId, FlightMap persistedStateMap)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    DbRetry.retryVoid(
        "flight.storePersistedStateMap",
        () -> storePersistedStateMapInner(flightId, persistedStateMap));
  }

  private void storePersistedStateMapInner(String flightId, FlightMap persistedStateMap)
      throws SQLException {
    final String sqlUpsert =
        "INSERT INTO "
            + FLIGHT_PERSISTED_TABLE
            + "(flightId, key, value) VALUES (:flightId, :key, :value1) "
            + "ON CONFLICT ON CONSTRAINT pk_flightpersisted "
            + "DO UPDATE SET value = :value2";

    List<FlightInput> inputList = FlightMapUtils.makeFlightInputList(persistedStateMap);

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sqlUpsert)) {
      startTransaction(connection);
      statement.setString("flightId", flightId);

      // Note that the unlike the Spring NamedParameterPreparedStatement code, the variant used
      // in Stairway only allows a single use of a substituted name. That is why we need value1
      // and value2 and give them the same value.
      for (FlightInput input : inputList) {
        statement.setString("key", input.getKey());
        statement.setString("value1", input.getValue());
        statement.setString("value2", input.getValue());
        statement.getPreparedStatement().executeUpdate();
      }
      commitTransaction(connection);
    }
  }

  /**
   * Given a flightId build the flight context from the database
   *
   * @param flightId identifier of the flight
   * @return constructed flight context
   * @throws StairwayException other stairway exception
   * @throws DatabaseOperationException database error
   * @throws InterruptedException thread shutdown
   */
  FlightContextImpl makeFlightContextById(String flightId)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry(
        "flight.makeFlightcontextById", () -> makeFlightContextByIdInner(flightId));
  }

  private FlightContextImpl makeFlightContextByIdInner(String flightId)
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
      FlightContextImpl flightContext = null;
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
  private FlightContextImpl makeFlightContext(Connection connection, String flightId, ResultSet rs)
      throws DatabaseOperationException, SQLException {

    FlightMap inputParameters = retrieveInputParameters(connection, flightId);
    PersistedStateMap persistedStateMap = retrievePersistedStateMap(connection, flightId);
    FlightContextLogState logState = makeLogState(connection, flightId);

    // Make debug info, if any
    FlightDebugInfo debugInfo = null;
    String debugInfoJson = rs.getString("debug_info");
    if (StringUtils.isNotEmpty(debugInfoJson)) {
      try {
        debugInfo =
            FlightDebugInfo.getObjectMapper().readValue(debugInfoJson, FlightDebugInfo.class);
      } catch (JsonProcessingException e) {
        throw new DatabaseOperationException(e);
      }
    }

    return new FlightContextImpl(
        flightId,
        rs.getString("class_name"),
        inputParameters,
        debugInfo,
        FlightStatus.valueOf(rs.getString("status")),
        logState,
        new ProgressMetersImpl(persistedStateMap));
  }

  /**
   * Build the log state of the flight context. The log state comprises the most recent log record
   * stored in FLIGHT_LOG_TABLE and the most recent working map stored in the FLIGHT_WORKING_TABLE.
   *
   * <p>For a newly created flight, there is no log record or data in the working table. In that
   * case, we return a log state with the initial values set. That will typically happen when the
   * caller has queued the flight on submit.
   *
   * @param connection database connection to use
   * @return FlightContextLogState either the log state from the database or the initial log state
   *     if there is nothing logged yet in the database.
   * @throws SQLException on database errors
   */
  private FlightContextLogState makeLogState(Connection connection, String flightId)
      throws SQLException {

    final String sqlLastFlightLog =
        "SELECT id, working_parameters, step_index, direction, rerun,"
            + " succeeded, serialized_exception, status"
            + " FROM "
            + FLIGHT_LOG_TABLE
            + " WHERE flightid = :flightId AND log_time = "
            + " (SELECT MAX(log_time) FROM "
            + FLIGHT_LOG_TABLE
            + " WHERE flightid = :flightId2)";

    try (NamedParameterPreparedStatement lastFlightLogStatement =
        new NamedParameterPreparedStatement(connection, sqlLastFlightLog)) {

      lastFlightLogStatement.setString("flightId", flightId);
      lastFlightLogStatement.setString("flightId2", flightId);

      try (ResultSet rsflight = lastFlightLogStatement.getPreparedStatement().executeQuery()) {
        if (!rsflight.next()) {
          // There is no row. Return the initial log state.
          return new FlightContextLogState(true); // true = set initial state
        }

        StepResult stepResult;
        if (rsflight.getBoolean("succeeded")) {
          stepResult = StepResult.getStepResultSuccess();
        } else {
          stepResult =
              new StepResult(
                  StepStatus.STEP_RESULT_FAILURE_FATAL,
                  exceptionSerializer.deserialize(rsflight.getString("serialized_exception")));
        }

        // TODO(PF-917): We may have JSON from working_parameters, a set of parameters from
        // flightworking table, neither, or both.  For now, delegate the decision of which to
        // use to FlightMap class.  PF-917 will remove column working_parameters.
        final String workingMapJson = rsflight.getString("working_parameters");
        final List<FlightInput> workingList =
            retrieveWorkingParameters(connection, rsflight.getObject("id", UUID.class));

        return new FlightContextLogState(false)
            .workingMap(FlightMapUtils.create(workingList, workingMapJson))
            .stepIndex(rsflight.getInt("step_index"))
            .rerun(rsflight.getBoolean("rerun"))
            .direction(Direction.valueOf(rsflight.getString("direction")))
            .result(stepResult);
      }
    }
  }

  /**
   * Return flight state for a single flight. The status of completed flight is limited by
   * completeFlightAvailable property.
   *
   * @param flightId flight to get
   * @return FlightState for the flight
   * @throws StairwayException - other Stairway error
   * @throws DatabaseOperationException - database error
   * @throws FlightNotFoundException - flightId is unknown to Stairway
   * @throws InterruptedException - interrupt
   */
  FlightState getFlightState(String flightId)
      throws StairwayException, DatabaseOperationException, FlightNotFoundException,
          InterruptedException {
    return DbRetry.retry("flight.getFlightState", () -> getFlightStateInner(flightId));
  }

  private FlightState getFlightStateInner(String flightId)
      throws SQLException, FlightNotFoundException, DatabaseOperationException {

    final String sqlOneFlight =
        "SELECT stairway_id, flightid, submit_time, "
            + " completed_time, output_parameters, status, serialized_exception, class_name"
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
   * <p>See {@link bio.terra.stairway.impl.FlightFilterAccess#makeSql()} for more information on the
   * shape of the sql that gets generated
   *
   * @param offset offset into the result set to start returning
   * @param limit max number of results to return
   * @param inFilter filters to apply to the flights
   * @return list of FlightState objects for the filtered, paged flights
   * @throws StairwayException other Stairway error
   * @throws DatabaseOperationException on all database issues
   * @throws InterruptedException thread shutdown
   */
  List<FlightState> getFlights(int offset, int limit, FlightFilter inFilter)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    FlightEnumeration enumeration =
        DbRetry.retry("flight.getFlights", () -> getFlightsInner(offset, limit, inFilter, null));
    return enumeration.getFlightStateList();
  }

  FlightEnumeration getFlights(
      @Nullable String nextPageToken, @Nullable Integer limit, @Nullable FlightFilter inFilter)
      throws StairwayException, DatabaseOperationException, InterruptedException {

    return DbRetry.retry(
        "flight.getFlights", () -> getFlightsInner(null, limit, inFilter, nextPageToken));
  }

  private FlightEnumeration getFlightsInner(
      Integer offset, Integer limit, FlightFilter inFilter, String nextPageToken)
      throws SQLException, StairwayException {
    // Make an empty filter if one is not provided
    FlightFilter filter = (inFilter != null) ? inFilter : new FlightFilter();

    // Make a filter access with no paging controls for the count query
    var countAccess = new FlightFilterAccess(filter, null, null, null);

    // Make another filter access including whatever paging controls we got before
    var stateAccess = new FlightFilterAccess(filter, offset, limit, nextPageToken);

    String countSql = countAccess.makeCountSql();
    String stateSql = stateAccess.makeSql();

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement flightCountStatement =
            new NamedParameterPreparedStatement(connection, countSql);
        NamedParameterPreparedStatement flightRangeStatement =
            new NamedParameterPreparedStatement(connection, stateSql)) {
      startReadOnlyTransaction(connection);

      countAccess.storePredicateValues(flightCountStatement);
      stateAccess.storePredicateValues(flightRangeStatement);

      int totalFlights;
      Instant currentTime;
      try (ResultSet rs = flightCountStatement.getPreparedStatement().executeQuery()) {
        rs.next();
        currentTime = rs.getTimestamp("currenttime").toInstant();
        totalFlights = rs.getInt("totalflights");
      }

      List<FlightState> flightStateList;
      try (ResultSet rs = flightRangeStatement.getPreparedStatement().executeQuery()) {
        flightStateList = makeFlightStateList(connection, rs);
      }

      // If we found some flights, then the next page token is the last submit time on
      // the list. If we are at the end of the list, then next page token starts at the
      // current time. We retrieve the time from the database to avoid skew between the
      // client time and the database server time.
      Instant nextPageTokenInstant;
      int listSize = flightStateList.size();
      if (listSize == 0) {
        nextPageTokenInstant = currentTime;
      } else {
        nextPageTokenInstant = flightStateList.get(listSize - 1).getSubmitted();
      }

      connection.commit();

      return new FlightEnumeration(
          totalFlights, new PageToken(nextPageTokenInstant).makeToken(), flightStateList);

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
      flightState.setClassName(rs.getString("class_name"));
      flightState.setInputParameters(retrieveInputParameters(connection, flightId));

      PersistedStateMap persistedStateMap = retrievePersistedStateMap(connection, flightId);
      ProgressMetersImpl meters = new ProgressMetersImpl(persistedStateMap);
      flightState.setProgressMeters(meters);

      // If the flight is in one of the complete states, then we retrieve the completion data
      if (flightState.getFlightStatus() == FlightStatus.SUCCESS
          || flightState.getFlightStatus() == FlightStatus.ERROR
          || flightState.getFlightStatus() == FlightStatus.FATAL) {
        flightState.setCompleted(rs.getTimestamp("completed_time").toInstant());
        flightState.setException(
            exceptionSerializer.deserialize(rs.getString("serialized_exception")));

        // TODO(PF-917): We may have JSON from output_parameters, a set of parameters from
        // flightworking table, neither, or both.  For now, delegate the decision of which to use to
        // FlightMap class.  PF-917 will remove column output_parameters.

        String outputParamsJson = rs.getString("output_parameters");
        final List<FlightInput> workingList = retrieveLatestWorkingParameters(connection, flightId);
        flightState.setResultMap(FlightMapUtils.create(workingList, outputParamsJson));
      }

      flightStateList.add(flightState);
    }

    return flightStateList;
  }

  private void storeInputParameters(
      Connection connection, String flightId, FlightMap inputParameters) throws SQLException {
    List<FlightInput> inputList = FlightMapUtils.makeFlightInputList(inputParameters);

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

  private void storeWorkingParameters(
      Connection connection, UUID logId, FlightMap workingParameters) throws SQLException {
    List<FlightInput> inputList = FlightMapUtils.makeFlightInputList(workingParameters);

    final String sqlInsertInput =
        "INSERT INTO "
            + FLIGHT_WORKING_TABLE
            + " (flightlog_id, key, value) VALUES (:logId, :key, :value)";

    try (NamedParameterPreparedStatement statement =
        new NamedParameterPreparedStatement(connection, sqlInsertInput)) {

      statement.setUuid("logId", logId);

      for (FlightInput input : inputList) {
        statement.setString("key", input.getKey());
        statement.setString("value", input.getValue());
        statement.getPreparedStatement().executeUpdate();
      }
    }
  }

  private FlightMap retrieveInputParameters(Connection connection, String flightId)
      throws SQLException {
    List<FlightInput> inputList = retrieveFlightInputs(FLIGHT_INPUT_TABLE, connection, flightId);
    var flightMap = new FlightMap();
    FlightMapUtils.fillInFlightMap(flightMap, inputList);
    return flightMap;
  }

  private PersistedStateMap retrievePersistedStateMap(Connection connection, String flightId)
      throws SQLException {
    List<FlightInput> inputList =
        retrieveFlightInputs(FLIGHT_PERSISTED_TABLE, connection, flightId);
    var persistedStateMap = new PersistedStateMap(this, flightId);
    FlightMapUtils.fillInFlightMap(persistedStateMap, inputList);
    return persistedStateMap;
  }

  // Common method for reading out flight map storage for input params and persisted state
  private List<FlightInput> retrieveFlightInputs(
      String tableName, Connection connection, String flightId) throws SQLException {
    final String sqlSelectInput =
        "SELECT flightId, key, value FROM " + tableName + " WHERE flightId = :flightId";

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

  private List<FlightInput> retrieveLatestWorkingParameters(Connection connection, String flightId)
      throws SQLException {
    final String sqlSelectInput =
        "SELECT key, value FROM "
            + FLIGHT_WORKING_TABLE
            + " WHERE flightlog_id="
            + "(SELECT id FROM flightlog WHERE (flightid = :flightId) AND log_time="
            + "   (SELECT MAX(log_time) FROM flightlog WHERE flightid = :flightId2))";

    List<FlightInput> inputList = new ArrayList<>();

    try (NamedParameterPreparedStatement statement =
        new NamedParameterPreparedStatement(connection, sqlSelectInput)) {

      statement.setString("flightId", flightId);
      statement.setString("flightId2", flightId);
      try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          FlightInput input = new FlightInput(rs.getString("key"), rs.getString("value"));
          inputList.add(input);
        }
      }
    }
    return inputList;
  }

  private List<FlightInput> retrieveWorkingParameters(Connection connection, UUID logId)
      throws SQLException {
    final String sqlSelectInput =
        "SELECT key, value FROM " + FLIGHT_WORKING_TABLE + " WHERE flightlog_id = :logId";

    List<FlightInput> inputList = new ArrayList<>();

    try (NamedParameterPreparedStatement statement =
        new NamedParameterPreparedStatement(connection, sqlSelectInput)) {
      statement.setUuid("logId", logId);

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
