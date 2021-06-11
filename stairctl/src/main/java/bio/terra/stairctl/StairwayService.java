package bio.terra.stairctl;

import bio.terra.stairctl.configuration.StairwayConfiguration;
import bio.terra.stairway.Stairway;
import java.util.Properties;
import javax.annotation.PostConstruct;
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

  @Autowired
  public StairwayService(StairwayConfiguration stairwayConfiguration) {
    this.stairwayConfiguration = stairwayConfiguration;
  }

  @PostConstruct
  public void initialization() {
    DataSource dataSource = configureDataSource();

    try {
      this.stairway =
          Stairway.newBuilder()
              .stairwayName(stairwayConfiguration.getName())
              .stairwayClusterName(stairwayConfiguration.getClusterName())
              .enableWorkQueue(false) // we will not be running any flights anywhere!
              .build();

      stairway.initialize(dataSource, false, false);
      System.out.println("Stairway initialized");
    } catch (Exception ex) {
      logger.error("Failed to initialize Stairway", ex);
      System.exit(1);
    }
  }

  public Stairway get() {
    return stairway;
  }

  private DataSource configureDataSource() {
    Properties props = new Properties();
    props.setProperty("user", stairwayConfiguration.getUsername());
    props.setProperty("password", stairwayConfiguration.getPassword());
    ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(stairwayConfiguration.getUri(), props);
    PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    return new PoolingDataSource<>(connectionPool);
  }
}
