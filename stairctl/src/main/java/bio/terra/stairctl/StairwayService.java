package bio.terra.stairctl;

import bio.terra.stairctl.configuration.StairwayConfiguration;
import bio.terra.stairway.Control;
import bio.terra.stairway.Stairway;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StairwayService {
  private static final Logger logger = LoggerFactory.getLogger(StairwayService.class);

  private final StairwayConfiguration stairwayConfiguration;
  private Stairway stairway;
  private Control control;
  private ConnectParams currentConnectParams;

  @Autowired
  public StairwayService(StairwayConfiguration stairwayConfiguration) {
    this.stairwayConfiguration = stairwayConfiguration;
  }

  public void connectStairway(ConnectParams connectParams) {
    // Fill in the defaults from the configuration
    connectParams.applyDefaults(stairwayConfiguration.makeConnectParams());
    DataSource dataSource = configureDataSource(connectParams);

    try {
      this.stairway =
          Stairway.newBuilder()
              .stairwayName("stairctl") // No queue, no flights, so name is irrelevant...
              .stairwayClusterName("stairctlcluster") // ...as is clustername
              .enableWorkQueue(false) // we will not be running any flights anywhere!
              .build();

      stairway.initialize(dataSource, false, false);

      // At this point, we have an initialized Stairway connected to its database.
      // This gives us the access we need to manipulate the database state.
      // We do not call the final step of Stairway startup: recoverAndStart().
      // We cannot, because we do not have the application classes that would be needed
      // to actually execute flights.

      control = stairway.getControl();
      currentConnectParams = connectParams;
      System.out.println("Connected to Stairway");
    } catch (Exception ex) {
      System.err.println("Failed to connect to Stairway or database: " + ex.getMessage());
      logger.error("Failed to connect to Stairway", ex);

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
    props.setProperty("user", connectParams.getUsername());
    props.setProperty("password", connectParams.getPassword());
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
