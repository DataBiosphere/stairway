package bio.terra.stairway;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Context for a flight. This contains the full state for a flight. It is what is held in the
 * database for the flight and it is passed into the steps
 */
public class FlightContext {
  /** The stairway instance running this flight */
  private Stairway stairway;

  /**
   * Identifier for the flight. The caller is expected to ensure that these are unique within this
   * Stairway.
   */
  private String flightId;

  /**
   * Full class name of the flight. This is used to reconstruct the flight from its persisted state.
   */
  private final String flightClassName;

  /** Unmodifiable flight map of the input parameters to the flight */
  private final FlightMap inputParameters;

  /**
   * Modifiable flight map used to communicate state between steps and convey output of the flight.
   */
  private final FlightMap workingMap;

  /** Index into the flight's step array of the step we are */
  private int stepIndex;

  /** Control flag to tell the flight runner to rerun the current step */
  private boolean rerun;

  /** Direction of execution: do, undo, switch. */
  private Direction direction;

  /** Returned status of the current step */
  private StepResult result;

  /** State of this flight */
  private FlightStatus flightStatus;

  /**
   * List of the class names of the steps. This is not used by the flight execution code. It is here
   * to allow more meaningful log messages by hooks.
   */
  private List<String> stepClassNames;

  /** Debug control of this flight. */
  private FlightDebugInfo debugInfo;

  /**
   * Dynamic list of step hooks defined for the current step, created using {@link
   * StairwayHook#stepFactory(FlightContext)}
   */
  private List<StepHook> stepHooks;

  /**
   * Dynamic list of light hooks defined for the current flight, created using {@link
   * StairwayHook#flightFactory(FlightContext)}
   */
  private List<FlightHook> flightHooks;

  // Construct the context with defaults
  public FlightContext(
      FlightMap inputParameters, String flightClassName, List<String> stepClassNames) {
    this.inputParameters = inputParameters;
    this.inputParameters.makeImmutable();
    this.flightClassName = flightClassName;
    this.workingMap = new FlightMap();
    this.stepIndex = 0;
    this.direction = Direction.START;
    this.result = StepResult.getStepResultSuccess();
    this.flightStatus = FlightStatus.RUNNING;
    this.stepClassNames = stepClassNames;
  }

  public String getFlightId() {
    return flightId;
  }

  public void setFlightId(String flightId) {
    this.flightId = flightId;
  }

  public String getFlightClassName() {
    return flightClassName;
  }

  public FlightMap getInputParameters() {
    return inputParameters;
  }

  // Normally, I don't hand out mutable maps, but in this case, the steps
  // will be making heavy use of the map. There does not seem to be a reason
  // to encapsulate it in this class.
  public FlightMap getWorkingMap() {
    return workingMap;
  }

  public int getStepIndex() {
    return stepIndex;
  }

  public void setStepIndex(int stepIndex) {
    this.stepIndex = stepIndex;
  }

  public FlightStatus getFlightStatus() {
    return flightStatus;
  }

  public void setFlightStatus(FlightStatus flightStatus) {
    this.flightStatus = flightStatus;
  }

  public boolean isRerun() {
    return rerun;
  }

  public void setRerun(boolean rerun) {
    this.rerun = rerun;
  }

  public Direction getDirection() {
    return direction;
  }

  public void setDirection(Direction direction) {
    this.direction = direction;
  }

  public boolean isDoing() {
    return (direction == Direction.DO || direction == Direction.START);
  }

  public StepResult getResult() {
    return result;
  }

  public void setResult(StepResult result) {
    this.result = result;
  }

  public Stairway getStairway() {
    return stairway;
  }

  public void setStairway(Stairway stairway) {
    this.stairway = stairway;
  }

  public List<String> getStepClassNames() {
    return stepClassNames;
  }

  public void setStepClassNames(List<String> stepClassNames) {
    this.stepClassNames = stepClassNames;
  }

  public String getStepClassName() {
    if (stepIndex < 0 || stepIndex >= stepClassNames.size()) {
      return StringUtils.EMPTY;
    }
    return stepClassNames.get(stepIndex);
  }

  public void setDebugInfo(FlightDebugInfo debugInfo) {
    this.debugInfo = debugInfo;
  }

  public FlightDebugInfo getDebugInfo() {
    return debugInfo;
  }

  public List<StepHook> getStepHooks() {
    return stepHooks;
  }

  public void setStepHooks(List<StepHook> stepHooks) {
    this.stepHooks = stepHooks;
  }

  public List<FlightHook> getFlightHooks() {
    return flightHooks;
  }

  public void setFlightHooks(List<FlightHook> flightHooks) {
    this.flightHooks = flightHooks;
  }

  /**
   * Set the step index to the next step. If we are doing, then we progress forwards. If we are
   * undoing, we progress backwards.
   */
  public void nextStepIndex() {
    if (!isRerun()) {
      switch (getDirection()) {
        case START:
          stepIndex = 0;
          setDirection(Direction.DO);
          break;
        case DO:
          stepIndex++;
          break;
        case UNDO:
          stepIndex--;
          break;
        case SWITCH:
          // run the undo of the current step
          break;
      }
    }
  }

  /**
   * Check the termination condition (either undo to 0 or do to stepListSize) depending on which
   * direction we are going.
   *
   * @param stepListSize size of the step list
   * @return true if there is a step to be executed
   */
  public boolean haveStepToDo(int stepListSize) {
    if (isDoing()) {
      return (stepIndex < stepListSize);
    } else {
      return (stepIndex >= 0);
    }
  }

  public String prettyStepState() {
    return "flight id: " + flightId + " step: " + stepIndex + " direction: " + direction;
  }

  public String flightDesc() {
    return "class: "
        + getFlightClassName()
        + " stairway: "
        + (getStairway() == null ? "<null>" : getStairway().getStairwayName())
        + " flightid: "
        + getFlightId();
  }

  @Override
  public String toString() {
    String debugString = debugInfo == null ? "" : debugInfo.toString();
    return new ToStringBuilder(this)
        .append("stairway", stairway)
        .append("flightId", flightId)
        .append("flightClassName", flightClassName)
        .append("inputParameters", inputParameters)
        .append("workingMap", workingMap)
        .append("stepIndex", stepIndex)
        .append("rerun", rerun)
        .append("direction", direction)
        .append("result", result)
        .append("flightStatus", flightStatus)
        .append("debugInfo", debugString)
        .toString();
  }
}
