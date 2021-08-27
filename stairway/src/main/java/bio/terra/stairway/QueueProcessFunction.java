package bio.terra.stairway;

/**
 * When Stairway calls the queue implementation to dispatch messages, it provides a function with
 * this signature. This function will attempt to activate the flight mentioned in the message. If
 * that succeeds, this function returns true and the message can be consumed. If that fails, this
 * function returns false and the message should remain on the queue. The typical reason for failure
 * is that the instance of the service is shutting down and can no longer run flights.
 */
@FunctionalInterface
public interface QueueProcessFunction {
  boolean apply(String message) throws InterruptedException;
}
