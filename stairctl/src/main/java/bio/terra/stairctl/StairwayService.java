package bio.terra.stairctl;

import bio.terra.stairctl.configuration.StairwayConfiguration;
import bio.terra.stairway.Control;
import bio.terra.stairway.Stairway;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class StairwayService {
  private static final Logger logger = LoggerFactory.getLogger(StairwayService.class);

  private final StairwayConfiguration stairwayConfiguration;
  private final ApplicationContext applicationContext;
  private Stairway stairway;
  private Control control;

  @Autowired
  public StairwayService(
      StairwayConfiguration stairwayConfiguration,
      ApplicationContext applicationContext) {
    this.stairwayConfiguration = stairwayConfiguration;
    this.applicationContext = applicationContext;
  }

  @PostConstruct
  @SuppressFBWarnings(value = "DM_EXIT", justification = "This is proper usage")
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
      control = stairway.getControl();
      System.out.println("Initialized Stairway");
    } catch (Exception ex) {
      System.err.println("Failed to initialize Stairway - exiting. Check the stairctl.log for details");
      logger.error("Failed to initialize Stairway", ex);
      SpringApplication.exit(applicationContext, () -> 1);
      //System.exit(1);
    }
  }

  public Stairway get() {
    return stairway;
  }

  public Control getControl() {
    return control;
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
