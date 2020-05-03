package bio.terra.stairway;

public class TestFlightRecoveryUndoSwitch extends Flight {

  public TestFlightRecoveryUndoSwitch(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Step 0 - increment
    addStep(new TestStepIncrement());

    // Step 1 - stop on undo - allow for recovery
    addStep(new TestStepIncrementStopUndo());
  }
}
