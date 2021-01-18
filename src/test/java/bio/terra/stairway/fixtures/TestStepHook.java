package bio.terra.stairway.fixtures;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StepHook;

public class TestStepHook implements StepHook {

  private final String hookId;
  private final TestHook testHook;

  TestStepHook(String hookId, TestHook testHook) {
    this.hookId = hookId;
    this.testHook = testHook;
  }

  @Override
  public HookAction startStep(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":stepHook:startStep");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endStep(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":stepHook:endStep");
    return HookAction.CONTINUE;
  }
}
