package bio.terra.stairway.flights;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/** A {@link Flight} that puts data in a working FlightMap that can't be recovered. */
public class TestFlightRecoveryUnrecoverableWorkingMap extends Flight {

  public TestFlightRecoveryUnrecoverableWorkingMap(
      FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    // Step 1 - increment
    addStep(new TestStepIncrement());

    // Step 2 - put unrecoverable value in the working map.
    addStep(new PutUnrecoverableValueStep());

    // Step 3 - stop - allow for failure
    addStep(new TestStepPause());

    // Step 4 - increment
    addStep(new TestStepIncrement());
  }

  /* Helper class to put a value in the working map that cannot be deserialized. */
  public static class PutUnrecoverableValueStep implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
      context.getWorkingMap().put("unrecoverable class", new PrivateConstructor("bar"));
      return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
      return StepResult.getStepResultSuccess();
    }
  }
  /**
   * The FlightMap serializer is able to serialize but not deserialize this class with a private
   * constructor.
   */
  private static final class PrivateConstructor {
    private final String foo;

    private PrivateConstructor(String foo) {
      this.foo = foo;
    }

    public String getFoo() {
      return foo;
    }
  }
}
