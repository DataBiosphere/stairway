package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;

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
}
