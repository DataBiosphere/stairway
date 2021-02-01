package bio.terra.stairway.fixtures;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightHook;
import bio.terra.stairway.HookAction;

public class TestFlightHook implements FlightHook {

  private final String hookId;
  private final TestHook testHook;

  TestFlightHook(String hookId, TestHook testHook) {
    this.hookId = hookId;
    this.testHook = testHook;
  }

  @Override
  public HookAction startFlight(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":flightHook:startFlight");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endFlight(FlightContext context) throws InterruptedException {
    testHook.addHookLog(hookId + ":flightHook:endFlight");
    return HookAction.CONTINUE;
  }
}
