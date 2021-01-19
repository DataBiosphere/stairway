package bio.terra.stairway;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StairwayThreadPool extends ThreadPoolExecutor {
  private static final Logger logger = LoggerFactory.getLogger(StairwayThreadPool.class);
  AtomicInteger activeTasks;

  public StairwayThreadPool(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    activeTasks = new AtomicInteger();
  }

  public int getActiveFlights() {
    return activeTasks.get();
  }

  public int getQueuedFlights() {
    return getQueue().size();
  }

  // TODO: make the logging debug or remove
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    int active = activeTasks.incrementAndGet();
    logger.debug("before: " + active);
  }

  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    int active = activeTasks.decrementAndGet();
    logger.debug("after: " + active);
  }
}
