package bio.terra.stairway;

import java.util.List;

/**
 * Read-only interface to the flight context. See FlightContextImpl for details.
 */
public interface FlightContext {
  /**
   * @return application context provided by the Stairway caller
   */
  Object getApplicationContext();

  /**
   * @return unique identifier of this flight; supplied by the caller
   */
  String getFlightId();

  /**
   * @return Java name of the flight class
   */
  String getFlightClassName();

  /**
   * @return immutable input parameter map
   */
  FlightMap getInputParameters();

  /**
   * @return mutable working map
   */
  FlightMap getWorkingMap();

  /**
   * @return zero-based index of the current step
   */
  int getStepIndex();

  /**
   * @return current flight status
   */
  FlightStatus getFlightStatus();

  /**
   * @return true if we are rerunning the current step; e.g., STEP_RESULT_RERUN
   */
  boolean isRerun();

  /**
   * @return direction of the step processing: START, DO, SWITCH, UNDO
   */
  Direction getDirection();

  /**
   * @return current step result
   */
  StepResult getResult();

  /**
   * @return containing Stairway instance
   */
  Stairway getStairway();

  /**
   * @return array of the names of all steps
   */
  List<String> getStepClassNames();

  /**
   * @return name of the current step being run, for logging
   */
  String getStepClassName();

  /**
   * @return pretty string describing the step state
   */
  String prettyStepState();

  /**
   * @return pretty string describing the flight
   */
  String flightDesc();
}
