package bio.terra.stairway.fixtures;

public class TestPauseController {
  private volatile int control;
  private static TestPauseController singleton = new TestPauseController();

  public TestPauseController() {
    control = 0;
  }

  public static int getControl() {
    return singleton.control;
  }

  public static void setControl(int control) {
    singleton.control = control;
  }
}
