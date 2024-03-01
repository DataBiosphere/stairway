package bio.terra.stairway.impl;

import bio.terra.stairway.FlightContext;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class StairwayThreadPool extends ThreadPoolExecutor {
  private static final Logger logger = LoggerFactory.getLogger(StairwayThreadPool.class);
  AtomicInteger activeTasks;

  StairwayThreadPool(int maxParallelFlights) {
    super(
        maxParallelFlights,
        maxParallelFlights,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>());
    activeTasks = new AtomicInteger();
  }

  int getActiveFlights() {
    return activeTasks.get();
  }

  int getQueuedFlights() {
    return getQueue().size();
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * get method will return null upon successful completion.
   *
   * @param flightRunner the flight to submit
   * @param flightContext
   * @return
   */
  protected Future<?> submitWithMdcAndFlightContext(
      Runnable flightRunner, FlightContext flightContext) {
    // Save the calling thread's context before potentially submitting to a child thread
    Map<String, String> callingThreadContext = MDC.getCopyOfContextMap();
    Runnable flightRunnerWithMdc =
        () -> {
          Map<String, String> initialContext = MDC.getCopyOfContextMap();
          initializeFlightMdc(callingThreadContext, flightContext);
          try {
            flightRunner.run();
          } finally {
            MdcUtils.overwriteContext(initialContext);
          }
        };
    return super.submit(flightRunnerWithMdc);
  }

  private void initializeFlightMdc(
      Map<String, String> callingThreadContext, FlightContext flightContext) {
    // Any leftover context on the thread will be fully overwritten:
    MdcUtils.overwriteContext(callingThreadContext);
    // If the calling thread's context contains flight and step context from a parent flight, this
    // will be overwritten below:
    MdcUtils.addFlightContextToMdc(flightContext);
    MdcUtils.removeStepContextFromMdc(flightContext);
  }

  protected void beforeExecute(Thread t, Runnable r) {
    int active = activeTasks.incrementAndGet();
    logger.debug("before: " + active);
    super.beforeExecute(t, r);
  }

  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    int active = activeTasks.decrementAndGet();
    logger.debug("after: " + active);
  }
}
