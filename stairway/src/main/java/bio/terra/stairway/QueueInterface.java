package bio.terra.stairway;

/**
 * In a cluster configuration (e.g., Kubernetes), multiple service instances containing Stairway
 * instances cooperate to provide the service. In that configuration, Stairway uses a queue to share
 * work across its instances and to move work during shutdown. Stairway requires a transactional
 * queue with at-least-once delivery semantics.
 *
 * <p>The queue interface is the abstraction Stairway uses to put messages onto the queue and read
 * them from the queue for processing. At the time of this writing, the only message passed is the
 * request for another Stairway instance to run a flight.
 *
 * <p>We need the queue abstraction to support multiple cloud platforms. On GCP, we use PubSub
 * queues. On Azure, there are several messaging options.
 */
public interface QueueInterface {

  /**
   * Dispatch messages from the work queue
   *
   * <p>Dispatching from the queue cannot be a simple dequeue operation. The queue implementations
   * on GCP and Azure are transactional. Getting an item from the cloud queue does not dequeue it.
   * The item is only removed from the queue if it is explicitly acknowledged. So rather than have a
   * simple dequeue method here, we call a cloud-specific dispatch method. It takes up to
   * maxMessages and calls the QueueProcessFunction. If the function returns true, the message is
   * acknowledged and removed from the queue. On failure, the message remains on the queue and will
   * be processed later.
   *
   * <p>In Stairway, processing the message means running a database transaction that changes the
   * state of a flight. The reliable sequence is:
   *
   * <ol>
   *   <li>Get a message from the queue
   *   <li>[A failure in this interval will leave the message on the queue]
   *   <li>Run the database transaction to change the flight state
   *   <li>[A transaction failure nacks the message, leaving it on the queue. A failure after the
   *       database commit and before the ack will cause the message to be delivered again.]
   *   <li>Ack the message. It is removed from the queue.
   *   <li>[There are race conditions within the queue that might lead to duplicate delivery. Those
   *       are arbitrated by the state in the database.]
   * </ol>
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
  void purgeQueueForTesting();
}
