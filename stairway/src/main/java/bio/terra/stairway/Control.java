package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * This class provides an interface for out-of-band debugging and recovering Stairway flights. It
 * caters to the use cases of the stairctl tool. The methods are constrained to only access the
 * database, and do not rely on any application-specific state. Once the FlightDao stops performing
 * deserialization, we may be able to share more methods with it. For now, at least, there is some
 * duplication of function between FlightDao and Control.
 */
public interface Control {
  int countFlights(FlightStatus status) throws SQLException;

  int countOwned() throws SQLException;

  List<Control.Flight> listFlightsSimple(int offset, int limit, FlightStatus status)
      throws SQLException;

  List<Control.Flight> listOwned(int offset, int limit) throws SQLException;

  Control.Flight getFlight(String flightId) throws SQLException;

  Control.Flight forceReady(String flightId)
      throws SQLException, StairwayException, InterruptedException;

  Control.Flight forceFatal(String flightId) throws SQLException;

  List<FlightMapEntry> inputQuery(String flightId) throws SQLException;

  List<Control.LogEntry> logQuery(String flightId) throws SQLException;

  List<String> listStairways() throws Exception;

  // -- POJOs for returning results --

  // Flight - represents a row from the "flight" table
  class Flight {
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
  class FlightMapEntry implements Comparable<FlightMapEntry> {
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
  class LogEntry implements Comparable<LogEntry> {
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
