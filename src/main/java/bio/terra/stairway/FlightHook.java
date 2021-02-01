package bio.terra.stairway;

public interface FlightHook {
  HookAction startFlight(FlightContext context) throws InterruptedException;

  HookAction endFlight(FlightContext context) throws InterruptedException;
}
