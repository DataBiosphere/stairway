package bio.terra.stairway;

/**
 * Dynamic hooks are created using factories specified by implementations of the StairwayHook
 * interface. See {@link StairwayHook} for more details.
 */
public interface DynamicHook {
  HookAction start(FlightContext context) throws InterruptedException;

  HookAction end(FlightContext context) throws InterruptedException;
}
