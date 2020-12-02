package bio.terra.stairway;

/** An interface to plugin code before and after each Flight step.  */
public interface StepHook {
  HookAction startStep(FlightContext flightContext) throws InterruptedException;

  HookAction endStep(FlightContext flightContext) throws InterruptedException;
}
