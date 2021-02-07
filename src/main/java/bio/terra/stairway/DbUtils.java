package bio.terra.stairway;

import java.sql.Connection;
import java.sql.SQLException;

/** Database methods shared across DAOs */
public class DbUtils {
  static void startTransaction(Connection connection) throws SQLException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    connection.setReadOnly(false);
  }

  static void startReadOnlyTransaction(Connection connection) throws SQLException {
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    connection.setReadOnly(true);
  }

  static void commitTransaction(Connection connection) throws SQLException {
    connection.commit();
  }
}
