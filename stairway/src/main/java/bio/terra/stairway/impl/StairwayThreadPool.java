package bio.terra.stairway.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StairwayThreadPool extends ThreadPoolExecutor {
  private static final Logger logger = LoggerFactory.getLogger(StairwayThreadPool.class);
  AtomicInteger activeTasks;

  StairwayThreadPool(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    activeTasks = new AtomicInteger();
  }

  int getActiveFlights() {
    return activeTasks.get();
  }

  int getQueuedFlights() {
    return getQueue().size();
  }

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
