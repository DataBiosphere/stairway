package bio.terra.stairway.impl;

import bio.terra.stairway.Direction;
import bio.terra.stairway.DynamicHook;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.ProgressMeter;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StairwayHook;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Context for a flight. This object contains all of the data associated with running the flight.
 * Some of the data is persisted and restored through the flight's lifecycle. Some is regenerated
 * (idempotently) by the Flight constructor. Some is dynamic based on the current instantiation of
 * the flight within a service instance.
 *
 * <p>A subset of the flight context is made visible via the FlightContext interface
 */
public class FlightContextImpl implements FlightContext {

  // -- dynamic state --
  // May be different for each instantiation of the flight

  /** The stairway instance running this flight */
  private StairwayImpl stairway;

  /** The client's application context, passed through to flights */
  private Object applicationContext;

  /**
   * Dynamic list of step hooks defined for the current step, created using {@link
   * StairwayHook#stepFactory(FlightContext)}
   */
  private List<DynamicHook> stepHooks;

  /**
   * Dynamic list of flight hooks defined for the current flight, created using {@link
   * StairwayHook#flightFactory(FlightContext)}
   */
  private List<DynamicHook> flightHooks;

  // -- regenerated state --
  // Copied from the Flight object. Must be reconstructed consistently for each
  // instantiation of the flight.

  /** Steps that comprise this flight */
  private List<Step> steps;

  /** RetryRules in parallel with the Steps list */
  private List<RetryRule> retryRules;

  /** Class names of the steps, for logging */
  private List<String> stepClassNames;

  // -- flight persisted state --
  // Persisted state about the flight as a whole

  /**
   * Identifier for the flight. The caller is expected to ensure that these are unique within this
   * Stairway.
   */
  private final String flightId;

  /**
   * Full class name of the flight. This is used to reconstruct the flight from its persisted state.
   */
  private final String flightClassName;

  /** Unmodifiable flight map of the input parameters to the flight */
  private final FlightMap inputParameters;

  /** Debug control of this flight. */
  private final FlightDebugInfo debugInfo;

  /** State of this flight */
  private FlightStatus flightStatus;

  // -- log persisted state --
  // Persisted state that changes each time a step is completed and its log record is written.
  private final FlightContextLogState logState;

  // -- persisted state --
  // Progress Meters
  private final ProgressMetersImpl progressMeters;

  /**
   * Context constructor used for new submitted flights. Initialize the context to start at the
   * first step (index 0).
   *
   * @param stairway the calling stairway object
   * @param flight the flight object with steps filled in
   * @param flightId unique id for this flight
   * @param debugInfo debugInfo for this flight
   */
  public FlightContextImpl(
      StairwayImpl stairway, Flight flight, String flightId, FlightDebugInfo debugInfo) {

    // Set the dynamic and regenerated state
    setDynamicContext(stairway, flight);

    // persisted state - in this case initialized for the start of a new flight
    this.flightId = flightId;
    this.flightClassName = flight.getClass().getName();
    this.inputParameters = flight.getInputParameters();
    this.inputParameters.makeImmutable();
    this.debugInfo = debugInfo;
    this.flightStatus = FlightStatus.RUNNING;
    this.logState = new FlightContextLogState(true); // true = fill in the defaults
    this.progressMeters = new ProgressMetersImpl(stairway.getFlightDao(), flightId);
  }

  /**
   * Context constructor used for persisted flights. Used in the DAO
   *
   * @param flightId unique id for this flight
   * @param flightClassName name of the flight class
   * @param inputParameters input parameters for the flight
   * @param debugInfo debugInfo for this flight
   * @param flightStatus current status of the flight
   * @param logState logged state of the last flight step
   * @param progressMeters progress meter state for this flight
   */
  public FlightContextImpl(
      String flightId,
      String flightClassName,
      FlightMap inputParameters,
      FlightDebugInfo debugInfo,
      FlightStatus flightStatus,
      FlightContextLogState logState,
      ProgressMetersImpl progressMeters) {
    this.flightId = flightId;
    this.flightClassName = flightClassName;
    this.inputParameters = inputParameters;
    this.inputParameters.makeImmutable();
    this.debugInfo = debugInfo;
    this.flightStatus = flightStatus;
    this.logState = logState;
    this.progressMeters = progressMeters;
  }

  public void setDynamicContext(StairwayImpl stairway, Flight flight) {
    // dynamic state
    this.stairway = stairway;
    this.applicationContext = flight.getApplicationContext();
    this.stepHooks = null;
    this.flightHooks = null;

    steps = flight.getSteps();
    retryRules = flight.getRetryRules();
    stepClassNames = new LinkedList<>();

    // regenerated state
    for (Step step : steps) {
      stepClassNames.add(step.getClass().getName());
    }
    // Make these lists immutable
    steps = Collections.unmodifiableList(steps);
    retryRules = Collections.unmodifiableList(retryRules);
    stepClassNames = Collections.unmodifiableList(stepClassNames);
  }

  // -- implementation of the FlightContext interface --

  @Override
  public Object getApplicationContext() {
    return applicationContext;
  }

  @Override
  public String getFlightId() {
    return flightId;
  }

  @Override
  public String getFlightClassName() {
    return flightClassName;
  }

  @Override
  public FlightMap getInputParameters() {
    return inputParameters;
  }

  @Override
  public FlightMap getWorkingMap() {
    return logState.getWorkingMap();
  }

  @Override
  public int getStepIndex() {
    return logState.getStepIndex();
  }

  @Override
  public FlightStatus getFlightStatus() {
    return flightStatus;
  }

  @Override
  public boolean isRerun() {
    return logState.isRerun();
  }

  @Override
  public Direction getDirection() {
    return logState.getDirection();
  }

  @Override
  public StepResult getResult() {
    return logState.getResult();
  }

  @Override
  public Stairway getStairway() {
    return stairway;
  }

  @Override
  public List<String> getStepClassNames() {
    return stepClassNames;
  }

  @Override
  public String getStepClassName() {
    if (getStepIndex() < 0 || getStepIndex() >= stepClassNames.size()) {
      return StringUtils.EMPTY;
    }
    return stepClassNames.get(getStepIndex());
  }

  @Override
  public String prettyStepState() {
    return "flight id: " + flightId + " step: " + getStepIndex() + " direction: " + getDirection();
  }

  @Override
  public String flightDesc() {
    return "class: "
        + getFlightClassName()
        + " stairway: "
        + (getStairway() == null ? "<null>" : getStairway().getStairwayName())
        + " flightid: "
        + getFlightId();
  }

  @Override
  public ProgressMeter getProgressMeter(String name) {
    return progressMeters.getProgressMeter(name);
  }

  /**
   * Set a progress meter and persist it in the Stairway database
   *
   * @param name name of the meter
   * @param v1 value1
   * @param v2 value2
   * @throws InterruptedException on interruption during database wait
   */
  @Override
  public void setProgressMeter(String name, long v1, long v2) throws InterruptedException {
    progressMeters.setProgressMeter(name, v1, v2);
  }

  // -- other accessors used in the implementation --

  StairwayImpl getStairwayImpl() {
    return stairway;
  }

  int getStepCount() {
    return steps.size();
  }

  Step getCurrentStep() {
    int stepIndex = getStepIndex();
    if (stepIndex < 0 || stepIndex >= getStepCount()) {
      throw new StairwayExecutionException("Invalid step index: " + stepIndex);
    }

    return steps.get(stepIndex);
  }

  RetryRule getCurrentRetryRule() {
    int stepIndex = getStepIndex();
    if (stepIndex < 0 || stepIndex >= getStepCount()) {
      throw new StairwayExecutionException("Invalid step index: " + stepIndex);
    }

    return retryRules.get(stepIndex);
  }

  FlightDebugInfo getDebugInfo() {
    return debugInfo;
  }

  // Check if we are 'doing' the last step in the flight.
  // Used to implement the DebugInfo last step failure in FlightRunner
  boolean isDoingLastStep() {
    return (isDoing() && getStepIndex() == getStepCount() - 1);
  }

  void nextStepIndex() throws InterruptedException {
    logState.nextStepIndex();
    // Update the stairway step progress meter to reflect what we step we are working on
    progressMeters.setStairwayStepProgress(getStepIndex(), getStepCount());
  }

  void setFlightStatus(FlightStatus flightStatus) {
    this.flightStatus = flightStatus;
  }

  void setRerun(boolean rerun) {
    logState.rerun(rerun);
  }

  void setDirection(Direction direction) {
    logState.direction(direction);
  }

  void setResult(StepResult result) {
    logState.result(result);
  }

  List<DynamicHook> getStepHooks() {
    return stepHooks;
  }

  void setStepHooks(List<DynamicHook> stepHooks) {
    this.stepHooks = stepHooks;
  }

  List<DynamicHook> getFlightHooks() {
    return flightHooks;
  }

  void setFlightHooks(List<DynamicHook> flightHooks) {
    this.flightHooks = flightHooks;
  }

  // -- execution methods -- maybe get rid of these

  boolean isDoing() {
    return logState.isDoing();
  }

  boolean haveStepToDo() {
    return logState.haveStepToDo(getStepCount());
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("stairway", stairway)
        .append("applicationContext", applicationContext)
        .append("stepHooks", stepHooks)
        .append("flightHooks", flightHooks)
        .append("steps", steps)
        .append("retryRules", retryRules)
        .append("stepClassNames", stepClassNames)
        .append("flightId", flightId)
        .append("flightClassName", flightClassName)
        .append("inputParameters", inputParameters)
        .append("debugInfo", debugInfo)
        .append("flightStatus", flightStatus)
        .append("logState", logState)
        .append("progressMeters", progressMeters)
        .toString();
  }
}
