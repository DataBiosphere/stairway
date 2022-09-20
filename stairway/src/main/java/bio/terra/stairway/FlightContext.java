package bio.terra.stairway;

import java.util.List;

/** Read-only interface to the flight context. See FlightContextImpl for details. */
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

  /**
   * Return meter data given the meter name
   *
   * @param name name of the meter to lookup
   * @return progress meter data or null if not found
   */
  ProgressMeter getProgressMeter(String name);

  /**
   * Set a progress meter for this flight. Typically, this is used in the form v1 operations are
   * complete out of v2 total operations.
   *
   * @param name name of the meter - should be unique across meters in the flight
   * @param v1 value 1
   * @param v2 value 2
   * @throws InterruptedException on interrupt during database wait
   */
  void setProgressMeter(String name, long v1, long v2) throws InterruptedException;
}
