package bio.terra.stairway.impl;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.MDC;

/**
 * Utility methods to make Stairway flight runnables context-aware, using mapped diagnostic context
 * (MDC).
 */
public class MdcUtils {
  /** ID of the flight */
  static final String FLIGHT_ID_KEY = "flightId";

  /** Class of the flight */
  static final String FLIGHT_CLASS_KEY = "flightClass";

  /** Class of the flight step */
  static final String FLIGHT_STEP_CLASS_KEY = "flightStepClass";

  /** Direction of the step (START, DO, SWITCH, or UNDO) */
  static final String FLIGHT_STEP_DIRECTION_KEY = "flightStepDirection";

  /** The step's execution order */
  static final String FLIGHT_STEP_NUMBER_KEY = "flightStepNumber";

  /**
   * Run and return the result of the callable with MDC's context map temporarily overwritten during
   * computation. The initial context map is then restored after computation.
   *
   * @param context to override MDC's context map
   * @param callable to call and return
   */
  public static <T> T callWithContext(Map<String, String> context, Callable<T> callable)
      throws InterruptedException {
    // Save the initial thread context so that it can be restored
    Map<String, String> initialContext = MDC.getCopyOfContextMap();
    try {
      MdcUtils.overwriteContext(context);
      System.out.println(MDC.getCopyOfContextMap());
      return callable.call();
    } catch (InterruptedException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new StairwayExecutionException("Unexpected exception " + ex.getMessage(), ex);
    } finally {
      MdcUtils.overwriteContext(initialContext);
    }
  }

  /**
   * Null-safe utility method for overwriting the current thread's MDC.
   *
   * @param context to set as MDC, if null then MDC will be cleared.
   */
  static void overwriteContext(Map<String, String> context) {
    MDC.clear();
    if (context != null) {
      MDC.setContextMap(context);
    }
  }

  private static Map<String, String> flightContextForMdc(FlightContext context) {
    return Map.of(
        FLIGHT_ID_KEY, context.getFlightId(),
        FLIGHT_CLASS_KEY, context.getFlightClassName());
  }

  /**
   * Supplement the current thread's MDC with flight-specific context, meant to persist for the
   * duration of this flight's execution on the thread.
   */
  static void addFlightContextToMdc(FlightContext flightContext) {
    flightContextForMdc(flightContext).forEach(MDC::put);
  }

  private static Map<String, String> stepContextForMdc(FlightContext flightContext) {
    return Map.of(
        FLIGHT_STEP_CLASS_KEY, flightContext.getStepClassName(),
        FLIGHT_STEP_DIRECTION_KEY, flightContext.getDirection().toString(),
        FLIGHT_STEP_NUMBER_KEY, Integer.toString(flightContext.getStepIndex()));
  }

  /**
   * Supplement the current thread's MDC with step-specific context, meant to persist for the
   * duration of this step's current attempt on the thread.
   */
  static void addStepContextToMdc(FlightContext flightContext) {
    stepContextForMdc(flightContext).forEach(MDC::put);
  }

  /** Remove any step-specific context from the current thread's MDC. */
  static void removeStepContextFromMdc(FlightContext flightContext) {
    stepContextForMdc(flightContext).keySet().forEach(MDC::remove);
  }
}
