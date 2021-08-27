package bio.terra.stairway;

public interface QueueInterface {

  /**
   * Read some messages from the queue and request that Stairway process each one.
   *
   * <p>This cannot be a simple dequeue operation. The queue implementations on GCP and Azure are
   * transactional. Getting an item from the cloud queue does not dequeue it. The item is only
   * removed from the queue if it is explicitly acknowledged. So rather than have a simple dequeue
   * method here, we call a cloud-specific dispatch method. It takes up to maxMessages and calls the
   * processFunction. If the function returns true, the message is acknowledged and removed from the
   * queue.
   *
   * <p>In Stairway, processing the message means running a database transaction that changes the
   * state of a flight.
   *
   * @param maxMessages Maximum messages to attempt to read at once. Fewer messages may be read.
   * @param processFunction Function to call for each message. The function returns true if the
   *     message is successfully handled; false if it was not and should remain on the queue.
   * @throws InterruptedException allows for queue waits to throw if the calling thread is shutdown
   */
  void dispatchMessages(int maxMessages, QueueProcessFunction processFunction)
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
