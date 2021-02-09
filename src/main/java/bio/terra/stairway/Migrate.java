package bio.terra.stairway;

import bio.terra.stairway.exception.MigrateException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This is a cut'n'paste of the template project Migrate. I don't know a good way to share
// that code
//  at this point in time. It seems like it should be shared though.
class Migrate {
  private final Logger logger = LoggerFactory.getLogger(Migrate.class);

  /**
   * Initialize drops existing tables in the database and reinitializes it with the changeset. This
   * is useful when developing or running integration tests, where there is no expectation that the
   * current state of the database is related to our changeset state.
   *
   * @param changesetFile - relative path to the changeset file in the project
   * @param dataSource - database to operate on
   * @throws MigrateException - wraps exceptions from liquibase and SQL
   */
  public void initialize(String changesetFile, DataSource dataSource) throws MigrateException {
    migrateWorker(changesetFile, dataSource, true);
  }

  /**
   * Upgrade applies changesets to move the current state of the database to a new state. This is
   * useful for alpha, staging, and production environments where we make some guarantee that the
   * database schema is in a valid state for applying changesets to.
   *
   * @param changesetFile - relative path to the changeset file in the project
   * @param dataSource - database to operate on
   * @throws MigrateException - wraps exceptions from liquibase and SQL
   */
  public void upgrade(String changesetFile, DataSource dataSource) throws MigrateException {
    migrateWorker(changesetFile, dataSource, false);
  }

  private void migrateWorker(String changesetFile, DataSource dataSource, boolean initialize)
      throws MigrateException {
    try (Connection connection = dataSource.getConnection()) {
      Liquibase liquibase =
          new Liquibase(
              changesetFile, new ClassLoaderResourceAccessor(), new JdbcConnection(connection));
      if (initialize) {
        logger.info("Initializing all tables in the database");
        liquibase.dropAll();
      }

      logger.info("Upgrading the database schema");
      liquibase.update(new Contexts()); // Run all migrations - no context filtering
    } catch (LiquibaseException | SQLException ex) {
      throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
    }
  }
}
