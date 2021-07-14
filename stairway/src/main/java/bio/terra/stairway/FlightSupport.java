package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayException;

/**
 * Flight support interface
 *
 * <p>This interface provides additional methods for the {@link Flight} implementation.
 * It lets us better separate the internal implementation of flights and the
 * Stairway object rather than have them as public members referenced directly from the Flight
 * object.
 */
public interface FlightSupport {
  // Hook methods
  void startFlightHook(FlightContext flightContext) throws InterruptedException;
  void endFlightHook(FlightContext flightContext) throws InterruptedException;
  void startStepHook(FlightContext flightContext) throws InterruptedException;
  void endStepHook(FlightContext flightContext) throws InterruptedException;

  // Shutdown state check
  boolean isQuietingDown();

  // Flight completion
  void exitFlight(FlightContext flightContext)
      throws StairwayException, InterruptedException;

  // Step completion
  void recordStep(FlightContext flightContext)
      throws StairwayException, InterruptedException;

  // Debug controls
  boolean getDebugRestartEachStep(FlightContext flightContext);
  StepResult debugStatusReplacement(
      FlightContext flightContext, int stepsSize, StepResult initialResult);
}
