package bio.terra.stairway;

public interface StairwayHook {
    HookAction startFlight(FlightContext context);
    HookAction startStep(FlightContext context);
    HookAction endFlight(FlightContext context);
    HookAction endStep(FlightContext context);
}
