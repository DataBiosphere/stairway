package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;

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

  private static final String flightSelectFrom =
      " flightid, submit_time, class_name, completed_time, status,"
          + "serialized_exception, stairway_id FROM flight ";

  private static final String flightOrderPage =
      " ORDER BY submit_time DESC OFFSET :offset LIMIT :limit";

  private int countQuery(NamedParameterPreparedStatement statement) throws SQLException {
    int flightCount = 0;
    try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
      while (rs.next()) {
        flightCount = rs.getInt("total");
      }
    }
    return flightCount;
  }

  public int countFlights(String status) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT COUNT(*) as total FROM flight");
    if (status != null) {
      try {
        FlightStatus.valueOf(status);
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException(
            "Invalid status value. Values are "
                + Arrays.stream(FlightStatus.values())
                    .map(FlightStatus::toString)
                    .collect(Collectors.joining(", ")));
      }
      sb.append(" WHERE status = :status");
    }

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      if (status != null) {
        statement.setString("status", status);
      }

      int flightCount = countQuery(statement);
      DbUtils.commitTransaction(connection);
      return flightCount;
    }
  }

  public int countOwned() throws SQLException {
    final String sql = "SELECT COUNT(*) AS total FROM flight WHERE stairway_id IS NOT NULL";
    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {
      DbUtils.startReadOnlyTransaction(connection);
      int flightCount = countQuery(statement);
      DbUtils.commitTransaction(connection);
      return flightCount;
    }
  }

  public List<Control.Flight> listFlightsSimple(int offset, int limit, String status)
      throws SQLException {

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(flightSelectFrom);
    if (status != null) {
      try {
        FlightStatus.valueOf(status);
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException(
            "Invalid status value. Values are "
                + Arrays.stream(FlightStatus.values())
                    .map(FlightStatus::toString)
                    .collect(Collectors.joining(", ")));
      }
      sb.append(" WHERE status = :status");
    }
    sb.append(flightOrderPage);

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      if (status != null) {
        statement.setString("status", status);
      }
      List<Control.Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public List<Control.Flight> listOwned(int offset, int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(flightSelectFrom);
    sb.append(" WHERE stairway_id IS NOT NULL ");
    sb.append(flightOrderPage);

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      List<Control.Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public Control.Flight getFlight(String flightId) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(flightSelectFrom);
    sb.append(" WHERE flightid = :flightid ");
    sb.append(flightOrderPage);

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sb.toString())) {
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

  public Control.Flight flightDisown(String flightId)
      throws SQLException, DatabaseOperationException, InterruptedException, FlightException {
    Control.Flight flight = getFlight(flightId);
    if (flight.getStairwayId().isEmpty()) {
      throw new IllegalStateException("Flight is not owned: " + flightId);
    }

    // Dummy up a flight context and use that to call the flight doa to disown.
    // When we have fixed serdes, we can use real flight contexts
    FlightMap fakeFlightMap = new FlightMap();
    FlightContext fakeFlightContext = new FlightContext(fakeFlightMap, null, null);
    fakeFlightContext.setFlightStatus(FlightStatus.READY);
    fakeFlightContext.setFlightId(flightId);
    flightDao.exit(fakeFlightContext);

    return getFlight(flightId);
  }

  public Control.Flight forceFatal(String flightId) throws SQLException {
    // We do not assume anything about the current state. We just force to a FATAL
    // disowned state so no attempt will be made to recover.
    // TODO: need a way to annotate what we did to the flight.
    final String sql =
        "UPDATE flight "
            + " SET completed_time = CURRENT_TIMESTAMP,"
            + " status = 'FATAL',"
            + " stairway_id = NULL"
            + " WHERE flightid = :flightid";

    Control.Flight flight = getFlight(flightId);
    if (flight.getStatus() == FlightStatus.FATAL) {
      throw new IllegalStateException("Flight is already fatal");
    }

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {
      statement.setString("flightid", flightId);
      DbUtils.startTransaction(connection);
      statement.getPreparedStatement().executeUpdate();
      DbUtils.commitTransaction(connection);
    }

    return getFlight(flightId);
  }

  public List<Control.KeyValue> inputQuery(String flightId) throws SQLException {
    final String sql = "SELECT key, value FROM flightinput WHERE flightid = :flightid";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement statement =
            new NamedParameterPreparedStatement(connection, sql)) {
      DbUtils.startReadOnlyTransaction(connection);
      statement.setString("flightid", flightId);
      List<Control.KeyValue> keyValueList = keyValueQuery(statement);
      DbUtils.commitTransaction(connection);
      return keyValueList;
    }
  }

  public List<Control.LogEntry> logQuery(String flightId) throws SQLException {
    final String sqllog =
        "SELECT log_time, step_index, status, serialized_exception,"
            + " rerun, direction, id FROM flightlog WHERE flightid = :flightid";

    final String sqlmap = "SELECT key, value FROM flightworking WHERE flightlog_id = :id";

    try (Connection connection = dataSource.getConnection();
        NamedParameterPreparedStatement logStatement =
            new NamedParameterPreparedStatement(connection, sqllog);
        NamedParameterPreparedStatement mapStatement =
            new NamedParameterPreparedStatement(connection, sqlmap)) {
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

      // We could do this as a join, but this is simpler and we reuse the keyValueQuery
      for (Control.LogEntry logEntry : logList) {
        mapStatement.setUuid("id", logEntry.getId());
        logEntry.workingMap(keyValueQuery(mapStatement));
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

  private List<Control.KeyValue> keyValueQuery(NamedParameterPreparedStatement statement)
      throws SQLException {

    List<Control.KeyValue> keyValueList = new ArrayList<>();
    try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
      while (rs.next()) {
        KeyValue keyValue = new KeyValue();
        keyValue.key(rs.getString("key")).value(rs.getString("value"));
        keyValueList.add(keyValue);
      }
    }
    return keyValueList;
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
  public static class KeyValue implements Comparable<KeyValue> {
    private String key;
    private String value;

    public String getKey() {
      return key;
    }

    public KeyValue key(String key) {
      this.key = key;
      return this;
    }

    public String getValue() {
      return value;
    }

    public KeyValue value(String value) {
      this.value = value;
      return this;
    }

    @Override
    public int compareTo(KeyValue o) {
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

      KeyValue keyValue = (KeyValue) o;

      if (key != null ? !key.equals(keyValue.key) : keyValue.key != null) {
        return false;
      }
      return value != null ? value.equals(keyValue.value) : keyValue.value == null;
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }
  }

  // Holds one log record with its working map
  // Comparable by log timestamp for a sequenced display
  public static class LogEntry implements Comparable<LogEntry> {
    private String flightId;
    private Instant logTime;
    private List<KeyValue> workingMap;
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

    public List<KeyValue> getWorkingMap() {
      return workingMap;
    }

    public LogEntry workingMap(List<KeyValue> workingMap) {
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

      if (stepIndex != logEntry.stepIndex) {
        return false;
      }
      if (rerun != logEntry.rerun) {
        return false;
      }
      if (flightId != null ? !flightId.equals(logEntry.flightId) : logEntry.flightId != null) {
        return false;
      }
      if (logTime != null ? !logTime.equals(logEntry.logTime) : logEntry.logTime != null) {
        return false;
      }
      if (workingMap != null
          ? !workingMap.equals(logEntry.workingMap)
          : logEntry.workingMap != null) {
        return false;
      }
      if (exception != null ? !exception.equals(logEntry.exception) : logEntry.exception != null) {
        return false;
      }
      if (direction != logEntry.direction) {
        return false;
      }
      return id != null ? id.equals(logEntry.id) : logEntry.id == null;
    }

    @Override
    public int hashCode() {
      int result = flightId != null ? flightId.hashCode() : 0;
      result = 31 * result + (logTime != null ? logTime.hashCode() : 0);
      result = 31 * result + (workingMap != null ? workingMap.hashCode() : 0);
      result = 31 * result + stepIndex;
      result = 31 * result + (exception != null ? exception.hashCode() : 0);
      result = 31 * result + (rerun ? 1 : 0);
      result = 31 * result + (direction != null ? direction.hashCode() : 0);
      result = 31 * result + (id != null ? id.hashCode() : 0);
      return result;
    }
  }
}
