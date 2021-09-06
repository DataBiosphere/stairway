package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.impl.StairwayImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * This builder class is the way to constructing a Stairway instance. For example,
 *
 * <pre>
 *   Stairway stairway = new StairwayBuilder()
 *     .maxParallelFlights(50)
 *     .maxQueuedFlights(10)
 *     ...
 *     .build();
 * </pre>
 */
public class StairwayBuilder {
  private final List<StairwayHook> stairwayHooks = new ArrayList<>();
  private Integer maxParallelFlights;
  private Integer maxQueuedFlights;
  private Object applicationContext;
  private ExceptionSerializer exceptionSerializer;
  private String stairwayName;
  private QueueInterface workQueue;
  private Duration retentionCheckInterval;
  private Duration completedFlightRetention;

  /**
   * Determines the size of the thread pool used for running Stairway flights. Default is
   * DEFAULT_MAX_PARALLEL_FLIGHTS (20 at this moment)
   *
   * @param maxParallelFlights maximum parallel flights to run
   * @return this
   */
  public StairwayBuilder maxParallelFlights(int maxParallelFlights) {
    this.maxParallelFlights = maxParallelFlights;
    return this;
  }

  public Integer getMaxParallelFlights() {
    return maxParallelFlights;
  }

  /**
   * If we are running a cluster work queue, then this parameter determines the number of flights we
   * allow to queue locally. If the queue depth is, larger than this max, then we stop taking work
   * off of the work queue and we add new submitted flights to the cluster work queue. Default is
   * DEFAULT_MAX_QUEUED_FLIGHTS (1 at this moment)
   *
   * <p>If maxQueuedFlights is smaller than MIN_QUEUED_FLIGHTS (0 at this moment), then it is set to
   * MIN_QUEUED_FLIGHTS. For the admission policy to work properly, MIN_QUEUED_FLIGHT <b>must</b> be
   * greater than 0.
   *
   * @param maxQueuedFlights maximum flights to queue in our local thread pool
   * @return this
   */
  public StairwayBuilder maxQueuedFlights(int maxQueuedFlights) {
    this.maxQueuedFlights = maxQueuedFlights;
    return this;
  }

  public Integer getMaxQueuedFlights() {
    return maxQueuedFlights;
  }

  /**
   * @param applicationContext application context passed along to Flight constructors. Default is
   *     null.
   * @return this
   */
  public StairwayBuilder applicationContext(Object applicationContext) {
    this.applicationContext = applicationContext;
    return this;
  }

  public Object getApplicationContext() {
    return applicationContext;
  }

  /**
   * @param exceptionSerializer Application-specific exception serializer. Default is the Stairway
   *     exception serializer
   * @return this
   */
  public StairwayBuilder exceptionSerializer(ExceptionSerializer exceptionSerializer) {
    this.exceptionSerializer = exceptionSerializer;
    return this;
  }

  public ExceptionSerializer getExceptionSerializer() {
    return exceptionSerializer;
  }

  /**
   * @param stairwayName Unique name of this Stairway instance. Used to identify which Stairway owns
   *     which flight. Default is "stairway" + random UUID
   * @return this
   */
  public StairwayBuilder stairwayName(String stairwayName) {
    this.stairwayName = stairwayName;
    return this;
  }

  public String getStairwayName() {
    return stairwayName;
  }

  /**
   * Work queue for this stairway. A Stairway cluster is defined by having the Stairway instances
   * share the same database and the same work queue.
   *
   * @param workQueue is an implementation of the QueueInterface class
   * @return this
   */
  public StairwayBuilder workQueue(QueueInterface workQueue) {
    this.workQueue = workQueue;
    return this;
  }

  public QueueInterface getWorkQueue() {
    return workQueue;
  }

  /**
   * Each call to stairwayHook adds hook object to a list of hooks. The hooks are processed in the
   * order in which they are added to the builder.
   *
   * @param stairwayHook object containing hooks for logging at beginning and end of flight and step
   *     of stairway
   * @return this
   */
  public StairwayBuilder stairwayHook(StairwayHook stairwayHook) {
    this.stairwayHooks.add(stairwayHook);
    return this;
  }

  public List<StairwayHook> getStairwayHooks() {
    return stairwayHooks;
  }

  /**
   * Control the frequency of clean up passes over the retained flights. Defaults to one day.
   *
   * @param retentionCheckInterval duration between retention checks for this stairway instance
   * @return this
   */
  public StairwayBuilder retentionCheckInterval(Duration retentionCheckInterval) {
    this.retentionCheckInterval = retentionCheckInterval;
    return this;
  }

  public Duration getRetentionCheckInterval() {
    return retentionCheckInterval;
  }

  /**
   * Specify the length of time that completed flights should be retained in the stairway database.
   * Defaults to retaining forever.
   *
   * @param completedFlightRetention duration before clean up
   * @return this
   */
  public StairwayBuilder completedFlightRetention(Duration completedFlightRetention) {
    this.completedFlightRetention = completedFlightRetention;
    return this;
  }

  public Duration getCompletedFlightRetention() {
    return completedFlightRetention;
  }

  /**
   * Construct a Stairway instance based on the builder inputs
   *
   * @return Stairway
   * @throws StairwayExecutionException on invalid input
   */
  public Stairway build() throws StairwayExecutionException {
    return new StairwayImpl(this);
  }
}
