package bio.terra.stairway.fixtures;

import bio.terra.stairway.DynamicHook;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;

public class TestStepHook implements DynamicHook {

  private final String hookId;
  private final TestHook testHook;

  TestStepHook(String hookId, TestHook testHook) {
    this.hookId = hookId;
    this.testHook = testHook;
  }

  @Override
  public HookAction start(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":stepHook:startStep");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction end(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":stepHook:endStep");
    return HookAction.CONTINUE;
  }
}
