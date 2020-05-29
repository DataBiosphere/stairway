package bio.terra.stairway;

public interface StairwayHook {
  HookAction startFlight(FlightContext context) throws InterruptedException;

  HookAction startStep(FlightContext context) throws InterruptedException;

  HookAction endFlight(FlightContext context) throws InterruptedException;

  HookAction endStep(FlightContext context) throws InterruptedException;
}
