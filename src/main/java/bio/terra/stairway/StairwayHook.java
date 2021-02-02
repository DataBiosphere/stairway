package bio.terra.stairway;

import java.util.Optional;

/**
 * When the {@link Stairway} object is built, you can specify one or more classes that implement
 * this StairwayHook interface.
 *
 * <p><b>Caution</b>
 *
 * <p>The hooks receive the live FlightContext on the flight thread. For correct operation of
 * Stairway, hooks must not modify the context or manipulate the thread.
 */
public interface StairwayHook {
  /**
   * The startFlight hook is called whenever a flight begins running on a Stairway thread. It may be
   * called more than once per flight, since a flight may start on a different thread due to
   * failover-recovery or due to a requested wait of some kind.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default HookAction startFlight(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  /**
   * The startStep hook called just before a step starts running on a Stairway thread. A given step
   * may be run more than once due to retrying and undo.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default HookAction startStep(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  /**
   * The endFlight hook called just before a flight stops running on a Stairway thread. As with
   * startFlight, it may be called more than once per flight.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default HookAction endFlight(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  /**
   * The endStep hook called just a step finishes running on a Stairway thread. A given step may be
   * run more than once due to retrying and undo.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default HookAction endStep(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  /**
   * The stateTransition hook is called <b>after</b> a flight state transition is recorded in the
   * Stairway database.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default HookAction stateTransition(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  /**
   * When a step is ready to be run, Stairway first calls the startStep hook above for each
   * configured StairwayHook. It then calls the stepFactory of each configured StairwayHook and
   * collects the resulting DynamicHook objects on a list stored in the flightContext. It then calls
   * the start() method of each of the dynamic hook objects.
   *
   * <p>When the step has completed, Stairway first calls the endStep hook for each configured
   * StairwayHook. It then calls the end() method of each of the dynamic hook objects and discards
   * the list.
   *
   * <p>A class implementing the DynamicHook interface can hold other state of interest to the
   * Stairway user, and recover that state when the end() method is called at the end of the step.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default Optional<DynamicHook> stepFactory(FlightContext context) throws InterruptedException {
    return Optional.empty();
  }

  /**
   * When a flight is ready to be (re)started on a Stairway thread, Stairway calls the startFlight
   * hook for each configured StairwayHook. It then calls the flightFactory of each configured
   * StairwayHooks and collects the resulting DynamicHook objects on a list stored in the
   * flightContext. It then calls the start() method of each of the dynamic hook objects.
   *
   * <p>When the flight exits, Stairway first calls the endStep hook for each configured
   * StairwayHook. It then calls the end() method of each of the dynamic hook objects and discards
   * the list.
   *
   * <p>A class implementing the DynamicHook interface can hold other state of interest to the
   * Stairway user, and recover that state when the end() method is called at the end of the flight.
   *
   * @param context the FlightContext of the flight
   * @return A HookAction. The only supported HookAction is CONTINUE
   * @throws InterruptedException when the thread is asked to exit due to Stairway termination
   */
  default Optional<DynamicHook> flightFactory(FlightContext context) throws InterruptedException {
    return Optional.empty();
  }
}
