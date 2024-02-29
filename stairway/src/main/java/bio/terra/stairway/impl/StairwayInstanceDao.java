package bio.terra.stairway.impl;

import static bio.terra.stairway.impl.DbUtils.commitTransaction;
import static bio.terra.stairway.impl.DbUtils.startReadOnlyTransaction;
import static bio.terra.stairway.impl.DbUtils.startTransaction;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.StairwayException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Database operations on the Stairway instance table */
class StairwayInstanceDao {
  private static final Logger logger = LoggerFactory.getLogger(StairwayInstanceDao.class);
  private static final String STAIRWAY_INSTANCE_TABLE = "stairwayinstance";

  private final DataSource dataSource;

  /**
   * Initialize the stairway instance dao with its database.
   *
   * @param dataSource database where the stairway tables live
   */
  StairwayInstanceDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Find or create a stairway instance. It is allowable to destroy and recreate a stairway instance
   * of the same name. It will recover flights that it owns.
   *
   * @param stairwayName string name of this stairway instance; must be unique across stairways in
   *     the database
   * @return short UUID for this stairway instance
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException on database errors
   * @throws InterruptedException on thread shutdown
   */
  String findOrCreate(String stairwayName)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry("stairwayInstance.findOrCreate", () -> findOrCreateInner(stairwayName));
  }

  private String findOrCreateInner(String stairwayName)
      throws SQLException, DatabaseOperationException {
    try (Connection connection = dataSource.getConnection()) {
      startTransaction(connection);

      String stairwayId = lookupQuery(connection, stairwayName);
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
        // This is Step 1 of removing use of a separate stairwayId and using the name
        // for the id everywhere. See src/main/resources/stairway/db/SCHEMA.md
        // for the migration plan.
        stairwayId = stairwayName;

        statement.setString("stairwayName", stairwayName);
        statement.setString("stairwayId", stairwayId);
        statement.getPreparedStatement().executeUpdate();

        commitTransaction(connection);
        return stairwayId;
      }
    }
  }

  /**
   * Given a stairway name, return the stairway id.
   *
   * @param stairwayName a string name for a stairway instance
   * @return the String id of the stairway instance
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException if the stairway instance was not found
   * @throws InterruptedException on thread shutdown
   */
  String lookupId(String stairwayName)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry("stairwayInstance.lookupId", () -> lookupIdInner(stairwayName));
  }

  private String lookupIdInner(String stairwayName)
      throws DatabaseOperationException, SQLException {
    try (Connection connection = dataSource.getConnection()) {
      startReadOnlyTransaction(connection);

      String stairwayId = lookupQuery(connection, stairwayName);

      commitTransaction(connection);
      return stairwayId;
    }
  }

  /**
   * Get a list of the stairway instance recorded in the stairway database.
   *
   * <p>This call can be used in conjunction with a list of the currently active stairway instances
   * to drive recovery of orphaned flights.
   *
   * @return list of the names of the stairway instances known to stairway
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException on SQL exception
   * @throws InterruptedException on thread shutdown
   */
  List<String> getList()
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return DbRetry.retry("stairwayInstance.getList", this::getListInner);
  }

  private List<String> getListInner() throws SQLException {
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
    }
    return instanceList;
  }

  /**
   * Inner query to delete a stairway instance by id. This is only used from FlightDao as part of
   * the recovery transaction. Retry and transaction boundaries are outside of this method. We have
   * it here so that this Java module encapsulates all access to the Stairway instance table.
   *
   * @param stairwayId the stairway id to delete from the instance table.
   */
  void delete(Connection connection, String stairwayId) throws SQLException {
    final String sqlDelete =
        "DELETE FROM " + STAIRWAY_INSTANCE_TABLE + " WHERE stairway_id = :stairwayId";

    try (NamedParameterPreparedStatement deleteStatement =
        new NamedParameterPreparedStatement(connection, sqlDelete)) {
      deleteStatement.setString("stairwayId", stairwayId);
      deleteStatement.getPreparedStatement().executeUpdate();
    }
  }

  private String lookupQuery(Connection connection, String stairwayName)
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
}
