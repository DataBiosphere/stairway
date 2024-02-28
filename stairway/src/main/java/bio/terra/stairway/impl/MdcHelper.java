package bio.terra.stairway.impl;

import bio.terra.stairway.FlightContext;
import org.slf4j.MDC;

import java.util.Map;

public class MdcHelper {

    /** ID of the flight */
    public static final String FLIGHT_ID_KEY = "flightId";

    /** Class of the flight */
    public static final String FLIGHT_CLASS_KEY = "flightClass";

    /** Class of the flight step */
    public static final String FLIGHT_STEP_CLASS_KEY = "flightStepClass";

    /** Direction of the step (START, DO, SWITCH, or UNDO) */
    public static final String FLIGHT_STEP_DIRECTION_KEY = "flightStepDirection";

    /** The step's execution order */
    public static final String FLIGHT_STEP_NUMBER_KEY = "flightStepNumber";

    /**
     * @param flightRunner
     * @param flightContext
     * @return the flightRunner modified to propagate the MDC from the calling thread and flight-specific context to the
     * child thread spawned to run the flight.
     */
    public static Runnable withMdcAndFlightContext(Runnable flightRunner, FlightContext flightContext) {
        // Save the calling thread's context
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        return () -> {
            MDC.setContextMap(contextMap);
            // If the calling thread's context contains flight and step context from a parent flight, this will
            // be overridden below:
            addFlightContextToMdc(flightContext);
            removeStepContextFromMdc(flightContext);
            try {
                flightRunner.run();
            } finally {
                // Once the flight is complete, clear MDC
                MDC.clear();
            }
        };
    }

    private static Map<String, String> flightContextForMdc(FlightContext context) {
        return Map.of(
                FLIGHT_ID_KEY, context.getFlightId(),
                FLIGHT_CLASS_KEY, context.getFlightClassName());
    }

    private static void addFlightContextToMdc(FlightContext context) {
        flightContextForMdc(context).forEach(MDC::put);
    }

    private static Map<String, String> stepContextForMdc(FlightContext context) {
        return Map.of(
                FLIGHT_STEP_CLASS_KEY, context.getStepClassName(),
                FLIGHT_STEP_DIRECTION_KEY, context.getDirection().toString(),
                FLIGHT_STEP_NUMBER_KEY, Integer.toString(context.getStepIndex()));
    }

    public static void addStepContextToMdc(FlightContext context) {
        stepContextForMdc(context).forEach(MDC::put);
    }

    public static void removeStepContextFromMdc(FlightContext context) {
        stepContextForMdc(context).keySet().forEach(MDC::remove);
    }
}
