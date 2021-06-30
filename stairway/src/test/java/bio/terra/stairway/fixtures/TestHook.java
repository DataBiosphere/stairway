package bio.terra.stairway.fixtures;

import bio.terra.stairway.DynamicHook;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StairwayHook;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHook implements StairwayHook {
  private static final Logger logger = LoggerFactory.getLogger(TestHook.class);
  private static final List<String> hookLog =
      Collections.synchronizedList(new LinkedList<String>());

  private final String hookId;

  public static List<String> getHookLog() {
    return hookLog;
  }

  public static void clearHookLog() {
    hookLog.clear();
  }

  public TestHook(String hookId) {
    this.hookId = hookId;
  }

  public void addHookLog(String log) {
    hookLog.add(log);
    logger.info("addHookLog: {}", log);
  }

  @Override
  public HookAction startFlight(FlightContext context) throws InterruptedException {
    addHookLog(hookId + ":startFlight");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction startStep(FlightContext context) throws InterruptedException {
    addHookLog(hookId + ":startStep");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endFlight(FlightContext context) throws InterruptedException {
    addHookLog(hookId + ":endFlight");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endStep(FlightContext context) throws InterruptedException {
    addHookLog(hookId + ":endStep");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction stateTransition(FlightContext context) throws InterruptedException {
    addHookLog(hookId + ":stateTransition:" + context.getFlightStatus().name());
    return HookAction.CONTINUE;
  }

  @Override
  public Optional<DynamicHook> stepFactory(FlightContext context) throws InterruptedException {
    return Optional.of(new TestStepHook(hookId, this));
  }

  @Override
  public Optional<DynamicHook> flightFactory(FlightContext context) throws InterruptedException {
    return Optional.of(new TestFlightHook(hookId, this));
  }
}
