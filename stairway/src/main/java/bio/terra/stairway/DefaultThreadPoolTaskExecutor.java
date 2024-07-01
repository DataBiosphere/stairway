package bio.terra.stairway;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class DefaultThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

  static final int DEFAULT_MAX_PARALLEL_FLIGHTS = 20;

  /**
   * An initialized {@link ThreadPoolTaskExecutor}.
   *
   * @param maxParallelFlights the desired pool size, honored if a positive integer, defaults to
   *     DEFAULT_MAX_PARALLEL_FLIGHTS otherwise.
   */
  public DefaultThreadPoolTaskExecutor(Integer maxParallelFlights) {
    super();
    int poolSize = getPoolSize(maxParallelFlights);
    super.setCorePoolSize(poolSize);
    super.setMaxPoolSize(poolSize);
    super.setKeepAliveSeconds(0);
    super.setThreadNamePrefix("stairway-thread-");
    super.initialize();
  }

  private int getPoolSize(Integer maxParallelFlights) {
    if (maxParallelFlights == null || maxParallelFlights <= 0) {
      return DEFAULT_MAX_PARALLEL_FLIGHTS;
    }
    return maxParallelFlights;
  }
}
