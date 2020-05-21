package bio.terra.stairway;

public interface StairwayHook {
    void startFlight(FlightContext context);
    void startStep();
    void endFlight();
    void endStep();
}
