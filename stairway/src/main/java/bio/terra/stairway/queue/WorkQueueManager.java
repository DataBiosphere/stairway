package bio.terra.stairway.queue;

import bio.terra.stairway.QueueInterface;
import bio.terra.stairway.impl.StairwayImpl;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QueueManager performs all control operations on the work queue
 */
public class WorkQueueManager {
  private static final Logger logger = LoggerFactory.getLogger(WorkQueueManager.class);

  private final StairwayImpl stairwayImpl;
  private final QueueInterface workQueue;
  private final boolean workQueueEnabled;

  private Thread workQueueListenerThread;

  public WorkQueueManager(StairwayImpl stairwayImpl, QueueInterface workQueue) {
    this.stairwayImpl = stairwayImpl;
    this.workQueue = workQueue;
    workQueueEnabled = (workQueue != null);
  }

  /**
   * The queue portion of the second phase of Stairway initialization.
   * @param forceClean if true, purge the contents of the queue; intended for test environments
   */
  public void initialize(boolean forceClean) {
    if (workQueueEnabled && forceClean) {
      workQueue.purgeQueue();
    }
  }

  /**
   * The queue portion of the third phase of Stairway initialization.
   * Start the work queue listener.
   */
  public void start() {
    if (workQueueEnabled) {
      WorkQueueListener workQueueListener = new WorkQueueListener(stairwayImpl, workQueue);
      workQueueListenerThread = new Thread(workQueueListener);
      workQueueListenerThread.start();
    }
  }

  /**
   * Start shutting down the queue during a graceful Stairway shutdown
   * @param waitSeconds seconds to allow for joining the work queue thread
   */
  public void shutdown(long waitSeconds) {
    tryTerminateListener(waitSeconds);
  }

  /**
   * Shutdown the queue with no wait, or at least try
   */
  public void shutdownNow() {
    tryTerminateListener(0);
  }

  public void queueReadyFlight(String flightId) throws StairwayExecutionException, InterruptedException {
    String message = QueueMessage.serialize(new QueueMessageReady(flightId));
    workQueue.enqueueMessage(message);
  }

  /**
   * Getter for work queue enabled.
   *
   * @return true if the work queue is enabled
   */
  public boolean isWorkQueueEnabled() {
    return workQueueEnabled;
  }

  private void tryTerminateListener(long waitSeconds) {
    if (workQueueEnabled) {
      logger.info("Shutting down work queue listener");
      // workQueueListener will notice quietingDown and stop, but it is often
      // waiting in sleep or in pull from queue and needs to be interrupted to terminate.
      workQueueListenerThread.interrupt();
      try {
        workQueueListenerThread.join(TimeUnit.SECONDS.toMillis(waitSeconds));
      } catch (InterruptedException ex) {
        logger.info("Failed to join work queue listener thread in the allotted time");
      }
      logger.info("Shutdown work queue listener");
    }
  }

}
