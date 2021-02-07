package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import java.sql.SQLException;

/**
 * We provide a custom functional interface to set the right thrown exceptions
 *
 * @param <R> The return value of the database function. If there is no return value, specify Void.
 */
@FunctionalInterface
public interface DbFunction<R> {
  R apply() throws SQLException, DatabaseOperationException, InterruptedException;
}
