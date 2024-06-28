package bio.terra.stairway.impl;

import bio.terra.stairway.FlightContext;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.MDC;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

class StairwayThreadPool {

  private final ThreadPoolTaskExecutor executor;

  StairwayThreadPool(ThreadPoolTaskExecutor executor) {
    this.executor = executor;
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
    return executor.submit(flightRunnerWithMdc);
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
}
