package bio.terra.stairway;

public interface StairwayHookV2 {

  /**
   * This is called once before every attempt to do or undo a step. A StepHook is used for exactly
   * one attempt.
   */
  StepHook newStep(FlightContext context);

  /**
   * An interface to allow clients to execute code before every do & undo of every step. A StepHook
   * instance is used only for a single attempt.
   */
  interface StepHook {
    /**
     * Called exactly once on the thread that will attempt a Step do or undo before that is done.
     */
    void start(FlightContext context);

    /** Called exactly once on the same thread as the Step after it attempts to do or undo. */
    void end(FlightContext context, StepResult stepResult);
  }

  // ditto, but per flight.
  FlightHook newFlight(FlightContext context);

  interface FlightHook {
    void start(FlightContext context);

    void end(FlightContext context);
  }
}
