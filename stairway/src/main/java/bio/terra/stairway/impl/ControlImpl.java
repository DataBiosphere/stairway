package bio.terra.stairway.impl;

import static bio.terra.stairway.impl.DbUtils.commitTransaction;

import bio.terra.stairway.Control;
import bio.terra.stairway.Direction;
import bio.terra.stairway.FlightStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

/** This class provides the implementation of {@link Control}. */
public class ControlImpl implements Control {
  private final DataSource dataSource;
  private final FlightDao flightDao;
  private final StairwayInstanceDao stairwayInstanceDao;

  ControlImpl(DataSource dataSource, FlightDao flightDao, StairwayInstanceDao stairwayInstanceDao) {
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

  public List<Flight> listFlightsSimple(int offset, int limit, FlightStatus status)
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
      List<Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public List<Flight> listOwned(int offset, int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(FLIGHT_SELECT_FROM);
    sb.append(" WHERE stairway_id IS NOT NULL ");
    sb.append(FLIGHT_ORDER_PAGE);

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sb.toString())) {
      DbUtils.startReadOnlyTransaction(connection);
      List<Flight> flightList = flightQuery(statement, offset, limit);
      DbUtils.commitTransaction(connection);
      return flightList;
    }
  }

  public Flight getFlight(String flightId) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT").append(FLIGHT_SELECT_FROM);
    sb.append(" WHERE flightid = :flightid ");
    sb.append(FLIGHT_ORDER_PAGE);

    try (var connection = dataSource.getConnection();
        var statement = new NamedParameterPreparedStatement(connection, sb.toString())) {
      statement.setString("flightid", flightId);
      DbUtils.startReadOnlyTransaction(connection);
      List<Flight> flightStateList = flightQuery(statement, 0, 1);
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
  public Flight forceReady(String flightId) throws SQLException {

    final String sql =
        "UPDATE flight SET status = 'READY', stairway_id = NULL WHERE flightid = :flightid";

    testFlightState(flightId, FlightStatus.READY);
    updateFlight(flightId, sql);
    return getFlight(flightId);
  }

  public Flight forceFatal(String flightId) throws SQLException {
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

  public List<LogEntry> logQuery(String flightId) throws SQLException {
    final String sqllog =
        "SELECT log_time, step_index, status, serialized_exception,"
            + " rerun, direction, id FROM flightlog WHERE flightid = :flightid";

    final String sqlmap = "SELECT key, value FROM flightworking WHERE flightlog_id = :id";

    try (var connection = dataSource.getConnection();
        var logStatement = new NamedParameterPreparedStatement(connection, sqllog);
        var mapStatement = new NamedParameterPreparedStatement(connection, sqlmap)) {
      DbUtils.startReadOnlyTransaction(connection);
      logStatement.setString("flightid", flightId);

      List<LogEntry> logList = new ArrayList<>();
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
      for (LogEntry logEntry : logList) {
        mapStatement.setUuid("id", logEntry.getId());
        logEntry.workingMap(flightMapQuery(mapStatement));
      }

      DbUtils.commitTransaction(connection);
      return logList;
    }
  }

  private List<Flight> flightQuery(NamedParameterPreparedStatement statement, int offset, int limit)
      throws SQLException {

    statement.setInt("offset", offset);
    statement.setInt("limit", limit);

    List<Flight> flightList = new ArrayList<>();
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
    Flight flight = getFlight(flightId);
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
}
