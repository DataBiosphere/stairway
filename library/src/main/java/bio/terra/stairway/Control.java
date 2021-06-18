package bio.terra.stairway;

import static bio.terra.stairway.DbUtils.commitTransaction;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * This class provides an API for out-of-band debugging and recovering Stairway flights. It caters
 * to the use cases of the stairctl tool. The methods are constrained to only access the database,
 * and do not rely on any application-specific state. Once the FlightDao stops performing
 * deserialization, we may be able to share more methods with it. For now, at least, there is some
 * duplication of function between FlightDao and Control.
 */
@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
public class Control {
  private final Stairway stairway;
  private final DataSource dataSource;
  private final FlightDao flightDao;
  private final StairwayInstanceDao stairwayInstanceDao;

  Control(
      Stairway stairway,
      DataSource dataSource,
      FlightDao flightDao,
      StairwayInstanceDao stairwayInstanceDao) {
    this.stairway = stairway;
    this.dataSource = dataSource;
    this.flightDao = flightDao;
    this.stairwayInstanceDao = stairwayInstanceDao;
  }

  // -- Flight methods --

  private static final String FLIGHT_SELECT_FROM =
      " flightid, submit_time, class_name, completed_time, status,"
          + "serialized_exception, stairway_id FROM flight ";

  private static final String FLIGHT_ORDER_PAGE =
      " ORDER BY submit_time DESC OFFSET :offset LIMIT :limit";

  private static final String FLIGHT_SELECT_COUNT = "SELECT COUNT(*) AS total FROM flight";

  private int countQuery(NamedParameterPreparedStatement statement) throws SQLException {
    int flightCount = 0;
    try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
      while (rs.next()) {
        flightCount = rs.getInt("total");
      }
    }
    return flightCount;
  }

  public int countFlights(FlightStatus status) throws SQLException {
    String sql;
    if (status == null) {
      sql = FLIGHT_SELECT_COUNT;
    } else {
      sql = FLIGHT_SELECT_COUNT + " WHERE status = :status";
    }

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sql)) {
      DbUtils.startReadOnlyTransaction(connection);
      if (status != null) {
        statement.setString("status", status.toString());
      }

      int flightCount = countQuery(statement);
      DbUtils.commitTransaction(connection);
      return flightCount;
    }
  }

  public int countOwned() throws SQLException {
    final String sql = FLIGHT_SELECT_COUNT + " WHERE stairway_id IS NOT NULL";
    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sql)) {
      DbUtils.startReadOnlyTransaction(connection);
      int flightCount = countQuery(statement);
      DbUtils.commitTransaction(connection);
      return flightCount;
    }
  }

  public List<Control.Flight> listFlightsSimple(int offset, int limit, FlightStatus status)
      throws SQLException {

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(FLIGHT_SELECT_FROM);
    if (status != null) {
      sb.append(" WHERE status = :status");
    }
    sb.append(FLIGHT_ORDER_PAGE);

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      if (status != null) {
        statement.setString("status", status.toString());
      }
      List<Control.Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public List<Control.Flight> listOwned(int offset, int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(FLIGHT_SELECT_FROM);
    sb.append(" WHERE stairway_id IS NOT NULL ");
    sb.append(FLIGHT_ORDER_PAGE);

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      List<Control.Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public Control.Flight getFlight(String flightId) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(FLIGHT_SELECT_FROM);
    sb.append(" WHERE flightid = :flightid ");
    sb.append(FLIGHT_ORDER_PAGE);

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sb.toString())) {
      statement.setString("flightid", flightId);
      DbUtils.startReadOnlyTransaction(connection);
      List<Control.Flight> flightStateList = flightQuery(statement, 0, 1);
      DbUtils.commitTransaction(connection);
      if (flightStateList.isEmpty()) {
        throw new IllegalArgumentException("Unknown flight id " + flightId);
      }
      return flightStateList.get(0);
    }
  }

  // We cannot use the regular disown code path, because it will only transition
  // from RUNNING --> READY. We want to force a transition from other states.
  // For example, restarting a dismal failure due to retry exhaustion.
  public Control.Flight forceReady(String flightId)
      throws SQLException, DatabaseOperationException, InterruptedException, FlightException {

    final String sql =
        "UPDATE flight SET status = 'READY', stairway_id = NULL WHERE flightid = :flightid";

    testFlightState(flightId, FlightStatus.READY);
    updateFlight(flightId, sql);
    return getFlight(flightId);
  }

  public Control.Flight forceFatal(String flightId) throws SQLException {
    // We do not assume anything about the current state. We just force to a FATAL
    // disowned state so no attempt will be made to recover.
    // TODO (PF-865): understand if we need a way to log/audit what we did to the flight.
    //  or perhaps just an annotation on the flight itself that it was modified.
    final String sql =
        "UPDATE flight "
            + " SET completed_time = CURRENT_TIMESTAMP,"
            + " status = 'FATAL',"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightid";

    testFlightState(flightId, FlightStatus.FATAL);
    updateFlight(flightId, sql);
    return getFlight(flightId);
  }

  public List<FlightMapEntry> inputQuery(String flightId) throws SQLException {
    final String sql = "SELECT key, value FROM flightinput WHERE flightid = :flightid";

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sql)) {
      DbUtils.startReadOnlyTransaction(connection);
      statement.setString("flightid", flightId);
      List<FlightMapEntry> flightMapEntries = flightMapQuery(statement);
      DbUtils.commitTransaction(connection);
      return flightMapEntries;
    }
  }

  public List<Control.LogEntry> logQuery(String flightId) throws SQLException {
    final String sqllog =
        "SELECT log_time, step_index, status, serialized_exception,"
            + " rerun, direction, id FROM flightlog WHERE flightid = :flightid";

    final String sqlmap = "SELECT key, value FROM flightworking WHERE flightlog_id = :id";

    try (var connection = dataSource.getConnection();
        var logStatement = new NamedParameterPreparedStatement(connection, sqllog);
        var mapStatement = new NamedParameterPreparedStatement(connection, sqlmap)) {
      DbUtils.startReadOnlyTransaction(connection);
      logStatement.setString("flightid", flightId);

      List<Control.LogEntry> logList = new ArrayList<>();
      try (ResultSet rs = logStatement.getPreparedStatement().executeQuery()) {
        while (rs.next()) {
          LogEntry logEntry = new LogEntry();
          logEntry
              .flightId(flightId)
              .logTime(rs.getTimestamp("log_time").toInstant())
              .stepIndex(rs.getInt("step_index"))
              .exception(rs.getString("serialized_exception"))
              .rerun(rs.getBoolean("rerun"))
              .direction(Direction.valueOf(rs.getString("direction")))
              .id(rs.getObject("id", UUID.class));
          logList.add(logEntry);
        }
      }

      // We could do this as a join, but this is simpler and we reuse the flightMapQuery
      for (Control.LogEntry logEntry : logList) {
        mapStatement.setUuid("id", logEntry.getId());
        logEntry.workingMap(flightMapQuery(mapStatement));
      }

      DbUtils.commitTransaction(connection);
      return logList;
    }
  }

  private List<Control.Flight> flightQuery(
      NamedParameterPreparedStatement statement, int offset, int limit) throws SQLException {

    statement.setInt("offset", offset);
    statement.setInt("limit", limit);

    List<Control.Flight> flightList = new ArrayList<>();
    try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
      while (rs.next()) {
        Flight flight = new Flight();
        flight
            .flightId(rs.getString("flightid"))
            .className(rs.getString("class_name"))
            .status(FlightStatus.valueOf(rs.getString("status")))
            .submitted(rs.getTimestamp("submit_time").toInstant())
            .completed(rs.getTimestamp("completed_time"))
            .exception(rs.getString("serialized_exception"))
            .stairwayId(rs.getString("stairway_id"));
        flightList.add(flight);
      }
    }
    return flightList;
  }

  private List<FlightMapEntry> flightMapQuery(NamedParameterPreparedStatement statement)
      throws SQLException {

    List<FlightMapEntry> flightMapEntries = new ArrayList<>();
    try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
      while (rs.next()) {
        FlightMapEntry entry = new FlightMapEntry();
        entry.key(rs.getString("key")).value(rs.getString("value"));
        flightMapEntries.add(entry);
      }
    }
    return flightMapEntries;
  }

  private void testFlightState(String flightId, FlightStatus status) throws SQLException {
    Control.Flight flight = getFlight(flightId);
    if (flight.getStatus() == status) {
      throw new IllegalStateException("Flight is already " + status.toString());
    }
  }

  private void updateFlight(String flightId, String sql) throws SQLException {
    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sql)) {

      DbUtils.startTransaction(connection);
      statement.setString("flightid", flightId);
      statement.getPreparedStatement().executeUpdate();
      commitTransaction(connection);
    }
  }

  // -- Stairway methods --

  public List<String> listStairways() throws Exception {
    return stairwayInstanceDao.getList();
  }

  // -- POJOs for returning results --

  // Flight - represents a row from the "flight" table
  public static class Flight {
    private String flightId;
    private String className;
    private FlightStatus status;
    private Instant submitted;
    private Instant completed;
    private String exception; // Un-deserialized exception text
    private String stairwayId;

    public String getFlightId() {
      return flightId;
    }

    public Flight flightId(String flightId) {
      this.flightId = flightId;
      return this;
    }

    public String getClassName() {
      return className;
    }

    public Flight className(String className) {
      this.className = className;
      return this;
    }

    public FlightStatus getStatus() {
      return status;
    }

    public Flight status(FlightStatus status) {
      this.status = status;
      return this;
    }

    public Instant getSubmitted() {
      return submitted;
    }

    public Flight submitted(Instant submitted) {
      this.submitted = submitted;
      return this;
    }

    public Optional<Instant> getCompleted() {
      return Optional.ofNullable(completed);
    }

    public Flight completed(Timestamp completed) {
      this.completed = Optional.ofNullable(completed).map(Timestamp::toInstant).orElse(null);
      return this;
    }

    public Optional<String> getException() {
      return Optional.ofNullable(exception);
    }

    public Flight exception(String exception) {
      this.exception = exception;
      return this;
    }

    public Optional<String> getStairwayId() {
      return Optional.ofNullable(stairwayId);
    }

    public Flight stairwayId(String stairwayId) {
      this.stairwayId = stairwayId;
      return this;
    }
  }

  // Holds flight map data as a pair of strings; either inputs or working map
  // Comparable by key for an alphabetical display
  public static class FlightMapEntry implements Comparable<FlightMapEntry> {
    private String key;
    private String value;

    public String getKey() {
      return key;
    }

    public FlightMapEntry key(String key) {
      this.key = key;
      return this;
    }

    public String getValue() {
      return value;
    }

    public FlightMapEntry value(String value) {
      this.value = value;
      return this;
    }

    @Override
    public int compareTo(FlightMapEntry o) {
      return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FlightMapEntry entry = (FlightMapEntry) o;
      return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  }

  // Holds one log record with its working map
  // Comparable by log timestamp for a sequenced display
  public static class LogEntry implements Comparable<LogEntry> {
    private String flightId;
    private Instant logTime;
    private List<FlightMapEntry> workingMap;
    private int stepIndex;
    private String exception;
    private boolean rerun;
    private Direction direction;
    private UUID id;

    public String getFlightId() {
      return flightId;
    }

    public LogEntry flightId(String flightId) {
      this.flightId = flightId;
      return this;
    }

    public Instant getLogTime() {
      return logTime;
    }

    public LogEntry logTime(Instant logTime) {
      this.logTime = logTime;
      return this;
    }

    public List<FlightMapEntry> getWorkingMap() {
      return workingMap;
    }

    public LogEntry workingMap(List<FlightMapEntry> workingMap) {
      this.workingMap = workingMap;
      return this;
    }

    public int getStepIndex() {
      return stepIndex;
    }

    public LogEntry stepIndex(int stepIndex) {
      this.stepIndex = stepIndex;
      return this;
    }

    public Optional<String> getException() {
      return Optional.ofNullable(exception);
    }

    public LogEntry exception(String exception) {
      this.exception = exception;
      return this;
    }

    public boolean isRerun() {
      return rerun;
    }

    public LogEntry rerun(boolean rerun) {
      this.rerun = rerun;
      return this;
    }

    public Direction getDirection() {
      return direction;
    }

    public LogEntry direction(Direction direction) {
      this.direction = direction;
      return this;
    }

    public UUID getId() {
      return id;
    }

    public LogEntry id(UUID id) {
      this.id = id;
      return this;
    }

    @Override
    public int compareTo(LogEntry o) {
      return this.logTime.compareTo(o.logTime);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogEntry logEntry = (LogEntry) o;
      return stepIndex == logEntry.stepIndex
          && rerun == logEntry.rerun
          && Objects.equals(flightId, logEntry.flightId)
          && Objects.equals(logTime, logEntry.logTime)
          && Objects.equals(workingMap, logEntry.workingMap)
          && Objects.equals(exception, logEntry.exception)
          && direction == logEntry.direction
          && Objects.equals(id, logEntry.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          flightId, logTime, workingMap, stepIndex, exception, rerun, direction, id);
    }
  }
}
