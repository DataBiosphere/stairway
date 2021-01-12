package bio.terra.stairway;

import java.util.Optional;

public interface StairwayHook {
  default HookAction startFlight(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  default HookAction startStep(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  default HookAction endFlight(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  default HookAction endStep(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }

  default HookAction stateTransition(FlightContext context) throws InterruptedException {
    return HookAction.CONTINUE;
  }
  default Optional<StepHook> stepFactory(FlightContext context) throws InterruptedException {
    return Optional.empty();
  }
}
