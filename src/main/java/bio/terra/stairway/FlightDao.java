package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightNotFoundException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The general layout of the stairway database tables is:
 * <ul>
 * <li>flight - records the flight, its inputs, and its outputs if any</li>
 * <li>flight log - records the steps of a running flight for recovery</li>
 * </ul>
 * This code assumes that the database is created and matches this codes schema
 * expectations. If not, we will crash and burn.
 * <p>
 * May want to split this into an interface and an implementation. This implementation
 * assumes Postgres.
 * <p>
 */
class FlightDao {

    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";
    private static String FLIGHT_INPUT_TABLE = "flightinput";

    private final DataSource dataSource;
    private final ExceptionSerializer exceptionSerializer;

    FlightDao(DataSource dataSource, ExceptionSerializer exceptionSerializer) {
        this.dataSource = dataSource;
        this.exceptionSerializer = exceptionSerializer;
    }

    /**
     * Truncate the tables
     */
    public void startClean() throws DatabaseSetupException {
        final String sqlTruncateTables = "TRUNCATE TABLE " +
                FLIGHT_TABLE + "," +
                FLIGHT_INPUT_TABLE + "," +
                FLIGHT_LOG_TABLE;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(sqlTruncateTables);

        } catch (SQLException ex) {
            throw new DatabaseSetupException("Failed to truncate database tables", ex);
        }
    }

    /**
     * Record a new flight
     */
    public void submit(FlightContext flightContext) throws DatabaseOperationException {
        final String sqlInsertFlight =
            "INSERT INTO " + FLIGHT_TABLE +
                " (flightId, submit_time, class_name, status)" +
                "VALUES (:flightid, CURRENT_TIMESTAMP, :class_name, :status)";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                 new NamedParameterPreparedStatement(connection, sqlInsertFlight)) {

            connection.setAutoCommit(false); // begin transaction
            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("class_name", flightContext.getFlightClassName());
            statement.setString("status", flightContext.getFlightStatus().name());
            statement.getPreparedStatement().executeUpdate();

            storeInputParameters(connection, flightContext.getFlightId(), flightContext.getInputParameters());

            connection.commit(); // commit transaction
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to create database tables", ex);
        }
    }

    /**
     * Record the flight state right before a step
     */
    public void step(FlightContext flightContext) throws DatabaseOperationException {
        final String sqlInsertFlightLog =
            "INSERT INTO " + FLIGHT_LOG_TABLE +
                "(flightid, log_time, working_parameters, step_index, doing," +
                " succeeded, serialized_exception, status)" +
                " VALUES (:flightid, CURRENT_TIMESTAMP, :working_map, :step_index, :doing," +
                " :succeeded, :serialized_exception, :status)";

        String serializedException =
            exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                 new NamedParameterPreparedStatement(connection, sqlInsertFlightLog)) {

            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("working_map", flightContext.getWorkingMap().toJson());
            statement.setInt("step_index", flightContext.getStepIndex());
            statement.setBoolean("doing", flightContext.isDoing());
            statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
            statement.setString("serialized_exception", serializedException);
            statement.setString("status", flightContext.getFlightStatus().name());
            statement.getPreparedStatement().executeUpdate();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to log step", ex);
        }
    }

    /**
     * Record completion of a flight and remove the detailed step data from the log table.
     * This is idempotent; repeated execution will work properly.
     */
    public void complete(FlightContext flightContext) throws DatabaseOperationException {
        // Make the update idempotent; that is, only do it if the status is RUNNING
        final String sqlUpdateFlight =
            "UPDATE " + FLIGHT_TABLE +
                " SET completed_time = CURRENT_TIMESTAMP," +
                " output_parameters = :output_parameters," +
                " status = :status," +
                " serialized_exception = :serialized_exception" +
                " WHERE flightid = :flightid AND status = 'RUNNING'";

        // The delete is harmless if it has been done before. We just won't find anything.
        final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid";

        String serializedException =
            exceptionSerializer.serialize(flightContext.getResult().getException().orElse(null));

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                 new NamedParameterPreparedStatement(connection, sqlUpdateFlight);
             NamedParameterPreparedStatement deleteStatement =
                 new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

            connection.setAutoCommit(false);

            statement.setString("output_parameters", flightContext.getWorkingMap().toJson());
            statement.setString("status", flightContext.getFlightStatus().name());
            statement.setString("serialized_exception", serializedException);
            statement.setString("flightid", flightContext.getFlightId());
            statement.getPreparedStatement().executeUpdate();

            deleteStatement.setString("flightid", flightContext.getFlightId());
            deleteStatement.getPreparedStatement().executeUpdate();

            connection.commit();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to complete flight", ex);
        }
    }

    /**
     * Remove all record of this flight from the database
     * @param flightId flight to remove
     * @throws DatabaseOperationException on any database error
     */
    public void delete(String flightId) throws DatabaseOperationException {
        final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid";
        final String sqlDeleteFlight = "DELETE FROM " + FLIGHT_TABLE + " WHERE flightid = :flightid";
        final String sqlDeleteFlightInput = "DELETE FROM " + FLIGHT_INPUT_TABLE + " WHERE flightid = :flightid";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement deleteFlightStatement =
                     new NamedParameterPreparedStatement(connection, sqlDeleteFlight);
             NamedParameterPreparedStatement deleteInputStatement =
                     new NamedParameterPreparedStatement(connection, sqlDeleteFlightInput);
             NamedParameterPreparedStatement deleteLogStatement =
                 new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

            connection.setAutoCommit(false);

            deleteFlightStatement.setString("flightid", flightId);
            deleteFlightStatement.getPreparedStatement().executeUpdate();

            deleteInputStatement.setString("flightid", flightId);
            deleteInputStatement.getPreparedStatement().executeUpdate();

            deleteLogStatement.setString("flightid", flightId);
            deleteLogStatement.getPreparedStatement().executeUpdate();

            connection.commit();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to delete flight", ex);
        }
    }

    /**
     * Find all active flights and return their flight contexts
     */
    public List<FlightContext> recover() throws DatabaseOperationException {
        final String sqlActiveFlights = "SELECT flightid, class_name " +
            " FROM " + FLIGHT_TABLE +
            " WHERE status = 'RUNNING'";

        final String sqlLastFlightLog = "SELECT working_parameters, step_index, doing," +
            " succeeded, serialized_exception, status" +
            " FROM " + FLIGHT_LOG_TABLE +
            " WHERE flightid = :flightid AND log_time = " +
            " (SELECT MAX(log_time) FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid2)";

        List<FlightContext> activeFlights = new LinkedList<>();

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement activeFlightsStatement =
                 new NamedParameterPreparedStatement(connection, sqlActiveFlights);
             NamedParameterPreparedStatement lastFlightLogStatement =
                 new NamedParameterPreparedStatement(connection, sqlLastFlightLog)) {

            connection.setAutoCommit(false);

            try (ResultSet rs = activeFlightsStatement.getPreparedStatement().executeQuery()) {
                while (rs.next()) {
                    String flightId = rs.getString("flightid");
                    List<FlightInput> inputList = retrieveInputParameters(connection, flightId);
                    FlightMap inputParameters = new FlightMap(inputList);
                    FlightContext flightContext = new FlightContext(inputParameters, rs.getString("class_name"));
                    flightContext.setFlightId(flightId);
                    activeFlights.add(flightContext);
                }
            }

            // Loop through the linked list making a query for each flight to fill in the FlightContext.
            // This may not be the most efficient algorithm. My reasoning is that the code is more obvious
            // to understand and this is not a performance-critical part of the processing; it happens once at startup.
            for (FlightContext flightContext : activeFlights) {

                lastFlightLogStatement.setString("flightid", flightContext.getFlightId());
                lastFlightLogStatement.setString("flightid2", flightContext.getFlightId());

                try (ResultSet rsflight = lastFlightLogStatement.getPreparedStatement().executeQuery()) {
                    // There may not be any log entries for a given flight. That happens if we fail after
                    // submit and before the first step. The defaults for flight context are correct for that
                    // case, so there is nothing left to do here.
                    if (rsflight.next()) {
                        StepResult stepResult;
                        if (rsflight.getBoolean("succeeded")) {
                            stepResult = StepResult.getStepResultSuccess();
                        } else {
                            stepResult = new StepResult(exceptionSerializer.deserialize(
                                rsflight.getString("serialized_exception")));
                        }

                        flightContext.getWorkingMap().fromJson(rsflight.getString("working_parameters"));

                        flightContext.setStepIndex(rsflight.getInt("step_index"));
                        flightContext.setDoing(rsflight.getBoolean("doing"));
                        flightContext.setResult(stepResult);
                        FlightStatus flightStatus = FlightStatus.valueOf(rsflight.getString("status"));
                        flightContext.setFlightStatus(flightStatus);
                    }
                }
            }

            connection.commit(); // The transaction is read-only, so commit is not needed, but feels cleaner...
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get active flight list", ex);
        }

        return activeFlights;
    }

    /**
     * Return flight state for a single flight
     *
     * @param flightId flight to get
     * @return FlightState for the flight
     */
    public FlightState getFlightState(String flightId) throws DatabaseOperationException {
        final String sqlOneFlight = "SELECT flightid, submit_time, " +
            " completed_time, output_parameters, status, serialized_exception" +
            " FROM " + FLIGHT_TABLE +
            " WHERE flightid = :flightid";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement oneFlightStatement =
                 new NamedParameterPreparedStatement(connection, sqlOneFlight)) {

            oneFlightStatement.setString("flightid", flightId);

            try (ResultSet rs = oneFlightStatement.getPreparedStatement().executeQuery()) {
                List<FlightState> flightStateList = makeFlightStateList(connection, rs);
                if (flightStateList.size() == 0) {
                    throw new FlightNotFoundException("Flight not found: " + flightId);
                }
                if (flightStateList.size() > 1) {
                    throw new DatabaseOperationException("Multiple flights with the same id?!");
                }
                return flightStateList.get(0);
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get flight", ex);
        }
    }

    public List<FlightState> getFlights(int offset, int limit) throws DatabaseOperationException {
        return getFlights(offset, limit, "", Collections.EMPTY_MAP);
    }

    private List<FlightState> getFlights(int offset, int limit, String whereClause, Map<String, String> whereParams)
        throws DatabaseOperationException {

        final String sqlFlightRange = "SELECT flightid, submit_time, " +
            " completed_time, output_parameters, status, serialized_exception" +
            " FROM " + FLIGHT_TABLE +
            whereClause +
            " ORDER BY submit_time" +  // should this be descending?
            " LIMIT :limit OFFSET :offset";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement flightRangeStatement =
                 new NamedParameterPreparedStatement(connection, sqlFlightRange)) {
            connection.setAutoCommit(false);

            flightRangeStatement.setInt("limit", limit);
            flightRangeStatement.setInt("offset", offset);

            for (Map.Entry<String, String> entry : whereParams.entrySet()) {
                flightRangeStatement.setString(entry.getKey(), entry.getValue());
            }

            List<FlightState> flightStateList;
            try (ResultSet rs = flightRangeStatement.getPreparedStatement().executeQuery()) {
                 flightStateList = makeFlightStateList(connection, rs);
            }

            connection.commit();
            return flightStateList;
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get flights", ex);
        }
    }

    private List<FlightState> makeFlightStateList(Connection connection, ResultSet rs)
            throws SQLException, DatabaseOperationException {
        List<FlightState> flightStateList = new ArrayList<>();

        while (rs.next()) {
            String flightId = rs.getString("flightid");
            FlightState flightState = new FlightState();

            // Flight data that is always present
            flightState.setFlightId(flightId);
            flightState.setFlightStatus(FlightStatus.valueOf(rs.getString("status")));
            flightState.setSubmitted(rs.getTimestamp("submit_time").toInstant());

            List<FlightInput> flightInput = retrieveInputParameters(connection, flightId);
            flightState.setInputParameters(new FlightMap(flightInput));

            if (flightState.getFlightStatus() != FlightStatus.RUNNING) {
                // If the optional flight data is present, then we fill it in
                flightState.setCompleted(rs.getTimestamp("completed_time").toInstant());
                flightState.setException(exceptionSerializer.deserialize(rs.getString("serialized_exception")));
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

    private void storeInputParameters(Connection connection, String flightId, FlightMap inputParameters) throws DatabaseOperationException {
        List<FlightInput> inputList = inputParameters.makeFlightInputList();

        final String sqlInsertInput =
                "INSERT INTO " + FLIGHT_INPUT_TABLE + " (flightId, key, value) VALUES (:flightid, :key, :value)";

        try (NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlInsertInput)) {

            statement.setString("flightid", flightId);

            for (FlightInput input : inputList) {
                statement.setString("key", input.getKey());
                statement.setString("value", input.getValue());
                statement.getPreparedStatement().executeUpdate();
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to insert input", ex);
        }
    }

    private List<FlightInput> retrieveInputParameters(Connection connection, String flightId)
            throws DatabaseOperationException {
        final String sqlSelectInput =
                "SELECT flightId, key, value FROM " + FLIGHT_INPUT_TABLE + " WHERE flightId = :flightid";

        List<FlightInput> inputList = new ArrayList<>();

        try (NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlSelectInput)) {
            statement.setString("flightid", flightId);

            try (ResultSet rs = statement.getPreparedStatement().executeQuery()) {
                while (rs.next()) {
                    FlightInput input = new FlightInput(rs.getString("key"), rs.getString("value"));
                    inputList.add(input);
                }
            }
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to select input", ex);
        }

        return inputList;
    }

}
