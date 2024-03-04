package bio.terra.stairway.fixtures;

import bio.terra.stairway.Direction;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.ProgressMeter;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

/**
 * A flight context implementation for use in unit tests to avoid running Stairway flights, with
 * additional helper methods for modification.
 */
public class TestFlightContext implements FlightContext {

  private String flightId = "flightId" + UUID.randomUUID();
  private String flightClassName = "flightClass" + UUID.randomUUID();
  private FlightMap inputParameters = new FlightMap();
  private FlightMap workingMap = new FlightMap();
  private int stepIndex = 0;
  private FlightStatus flightStatus = FlightStatus.QUEUED;
  private Direction direction = Direction.DO;
  private String stepClassName = "stepClass" + UUID.randomUUID();

  @Override
  public Object getApplicationContext() {
    return null;
  }

  @Override
  public String getFlightId() {
    return flightId;
  }

  public TestFlightContext flightId(String flightId) {
    this.flightId = flightId;
    return this;
  }

  @Override
  public String getFlightClassName() {
    return flightClassName;
  }

  public TestFlightContext flightClassName(String flightClassName) {
    this.flightClassName = flightClassName;
    return this;
  }

  @Override
  public FlightMap getInputParameters() {
    return inputParameters;
  }

  public TestFlightContext inputParameters(FlightMap inputParameters) {
    this.inputParameters = inputParameters;
    return this;
  }

  @Override
  public FlightMap getWorkingMap() {
    return workingMap;
  }

  @Override
  public int getStepIndex() {
    return stepIndex;
  }

  public TestFlightContext stepIndex(int stepIndex) {
    this.stepIndex = stepIndex;
    return this;
  }

  @Override
  public FlightStatus getFlightStatus() {
    return flightStatus;
  }

  public TestFlightContext flightStatus(FlightStatus flightStatus) {
    this.flightStatus = flightStatus;
    return this;
  }

  @Override
  public boolean isRerun() {
    return false;
  }

  @Override
  public Direction getDirection() {
    return direction;
  }

  public TestFlightContext direction(Direction direction) {
    this.direction = direction;
    return this;
  }

  @Override
  public StepResult getResult() {
    return null;
  }

  @Override
  public Stairway getStairway() {
    return null;
  }

  @Override
  public List<String> getStepClassNames() {
    return null;
  }

  @Override
  public String getStepClassName() {
    return stepClassName;
  }

  public TestFlightContext stepClassName(String stepClassName) {
    this.stepClassName = stepClassName;
    return this;
  }

  @Override
  public String prettyStepState() {
    return null;
  }

  @Override
  public String flightDesc() {
    return null;
  }

  @Override
  public ProgressMeter getProgressMeter(String name) {
    return null;
  }

  @Override
  public void setProgressMeter(String name, long v1, long v2) {}
}
