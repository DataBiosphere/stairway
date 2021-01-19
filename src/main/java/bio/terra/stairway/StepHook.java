package bio.terra.stairway;

public interface StepHook {
  HookAction startStep(FlightContext context) throws InterruptedException;

  HookAction endStep(FlightContext context) throws InterruptedException;
}
