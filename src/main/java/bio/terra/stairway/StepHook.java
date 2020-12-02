package bio.terra.stairway;

public interface StepHook {
  HookAction startStep(FlightContext flightContext);
  HookAction endStep(FlightContext flightContext);
}
