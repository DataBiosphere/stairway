package bio.terra.stairctl;

import bio.terra.stairctl.commands.Output;
import bio.terra.stairctl.configuration.StairwayConfiguration;
import bio.terra.stairway.Control;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StairwayBuilder;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StairwayService {
  private final StairwayConfiguration stairwayConfiguration;
  private final Output output;
  private Stairway stairway;
  private Control control;
  private ConnectParams currentConnectParams;

  @Autowired
  public StairwayService(StairwayConfiguration stairwayConfiguration, Output output) {
    this.stairwayConfiguration = stairwayConfiguration;
    this.output = output;
  }

  public void connectStairway(ConnectParams connectParams) {
    // Fill in the defaults from the configuration
    connectParams = connectParams.withDefaults(stairwayConfiguration.makeConnectParams());
    DataSource dataSource = configureDataSource(connectParams);

    try {
      this.stairway =
          new StairwayBuilder()
              .stairwayName("stairctl") // No queue, no flights, so name is irrelevant...
              .build();

      stairway.initialize(dataSource, false, false);

      // At this point, we have an initialized Stairway connected to its database.
      // This gives us the access we need to manipulate the database state.
      // We do not call the final step of Stairway startup: recoverAndStart().
      // We cannot, because we do not have the application classes that would be needed
      // to actually execute flights.

      control = stairway.getControl();
      currentConnectParams = connectParams;
      output.println("Connected to Stairway");
    } catch (Exception ex) {
      output.error(
          "Failed to connect to Stairway or database using connection: " + connectParams, ex);
    }
  }

  public void disconnectStairway() {
    this.control = null;
    this.stairway = null;
    this.currentConnectParams = null;
  }

  public Stairway get() {
    return stairway;
  }

  public Control getControl() {
    return control;
  }

  public ConnectParams getCurrentConnectParams() {
    return currentConnectParams;
  }

  public boolean isConnected() {
    return (control != null);
  }

  private DataSource configureDataSource(ConnectParams connectParams) {
    Properties props = new Properties();
    props.setProperty("user", connectParams.username());
    props.setProperty("password", connectParams.password());
    ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(connectParams.makeUri(), props);
    PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    return new PoolingDataSource<>(connectionPool);
  }
}
