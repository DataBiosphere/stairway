package bio.terra.stairway;

import bio.terra.stairway.exception.RetryException;

/**
 * Step must implement a do and an undo method. The do performs the step and the undo removes the
 * effects of the step. Care must be taken in their implementation to make sure that a failure at
 * any point can be either continued or undone. The best way to do that is to make each step do only
 * one thing.
 *
 * <p>There are five cases for calls to do and undo:
 *
 * <ul>
 *   <li>do fails and the retry rule says retry, so we call do again
 *   <li>do fails and the retry rule says fail, so we switch to undoing and call the undo method of
 *       the failed do
 *   <li>undo fails and the retry rule says retry, so we call undo again
 *   <li>undo fails and the retry rule says fail. We declare the flight a 'dismal failure'. That is
 *       persisted. This case would require manual cleanup.
 *   <li>The drmanager fails. On restart, all incomplete flights are continued doing whatever they
 *       were doing.
 * </ul>
 *
 * I think that is the right behavior, but please let me know.
 */
public interface Step {
  /**
   * Called by the Flight controller when running "forward" on the success path
   *
   * @param context The sequencer context
   * @return step result object
   * @throws InterruptedException when the thread pool is being shut down
   * @throws RetryException may through retry exception, causing the Flight machinery to run the
   *     RetryRule associated with the step.
   */
  StepResult doStep(FlightContext context) throws InterruptedException, RetryException;

  /**
   * Called by the sequencer when running "backward" on the failure/rollback path
   *
   * @param context The sequencer context
   * @return step result object
   * @throws InterruptedException when the thread pool is being shut down
   */
  StepResult undoStep(FlightContext context) throws InterruptedException;
}
