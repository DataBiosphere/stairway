package bio.terra.stairway;

public interface QueueInterface {

  /**
   * Read some messages from the queue and request that Stairway process each one.
   *
   * @param dispatchContext anonymous context passed to the processFunction
   * @param maxMessages Maximum messages to attempt to read at once. Fewer messages may be read.
   * @param processFunction Function to call for each message. The function returns true if the
   *     message is successfully handled; false if it was not and should remain on the queue.
   * @throws InterruptedException allows for queue waits to throw if the calling thread is shutdown
   */
  void dispatchMessages(
      Object dispatchContext, int maxMessages, QueueProcessFunction processFunction)
      throws InterruptedException;

  /**
   * Put a message on the queue.
   *
   * @param message the message to enqueue
   * @throws InterruptedException allows for queue waits to throw if the calling thread is shutdown
   */
  void enqueueMessage(String message) throws InterruptedException;

  /**
   * Remove all messages from a queue. NOTE: This method is intended for controlled test
   * environments where a cloud platform queue is reused. It should clear all messages from the
   * queue, simply ignoring them, so that tests can start from an empty queue.
   */
  void purgeQueue();
}
