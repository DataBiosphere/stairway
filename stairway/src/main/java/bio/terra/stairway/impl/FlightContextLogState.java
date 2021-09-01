package bio.terra.stairway.impl;

import bio.terra.stairway.Direction;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This POJO represents the part of the flight context that changes each time a step is completed
 * and its log record is written. It is factored out as a separate POJO to simplify handling in the
 * DAO and initializing the flight context object.
 */
public class FlightContextLogState {
  /**
   * Modifiable flight map used to communicate state between steps and convey output of the flight.
   */
  private FlightMap workingMap;

  /** Index into the flight's step array of the step we are */
  private int stepIndex;

  /** Control flag to tell the flight runner to rerun the current step */
  private boolean rerun;

  /** Direction of execution: do, undo, switch. */
  private Direction direction;

  /** Returned status of the current step */
  private StepResult result;

  /**
   * Constructor that can optionally set defaults for the log state
   *
   * @param setDefaults true to set default; false otherwise
   */
  public FlightContextLogState(boolean setDefaults) {
    if (setDefaults) {
      this.workingMap = new FlightMap();
      this.stepIndex = 0;
      this.rerun = false;
      this.direction = Direction.START;
      this.result = StepResult.getStepResultSuccess();
    }
  }

  public FlightMap getWorkingMap() {
    return workingMap;
  }

  public FlightContextLogState workingMap(FlightMap workingMap) {
    this.workingMap = workingMap;
    return this;
  }

  public int getStepIndex() {
    return stepIndex;
  }

  public FlightContextLogState stepIndex(int stepIndex) {
    this.stepIndex = stepIndex;
    return this;
  }

  public boolean isRerun() {
    return rerun;
  }

  public FlightContextLogState rerun(boolean rerun) {
    this.rerun = rerun;
    return this;
  }

  public Direction getDirection() {
    return direction;
  }

  public FlightContextLogState direction(Direction direction) {
    this.direction = direction;
    return this;
  }

  public StepResult getResult() {
    return result;
  }

  public FlightContextLogState result(StepResult result) {
    this.result = result;
    return this;
  }

  // -- execution methods --

  boolean isDoing() {
    return (direction == Direction.DO || direction == Direction.START);
  }

  /**
   * Set the step index to the next step. If we are doing, then we progress forwards. If we are
   * undoing, we progress backwards.
   */
  void nextStepIndex() {
    if (!rerun) {
      switch (direction) {
        case START:
          stepIndex = 0;
          direction = Direction.DO;
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
  boolean haveStepToDo(int stepListSize) {
    if (isDoing()) {
      return (stepIndex < stepListSize);
    } else {
      return (stepIndex >= 0);
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("workingMap", workingMap)
        .append("stepIndex", stepIndex)
        .append("rerun", rerun)
        .append("direction", direction)
        .append("result", result)
        .toString();
  }
}
