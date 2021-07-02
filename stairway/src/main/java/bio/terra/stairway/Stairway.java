package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.DuplicateFlightIdSubmittedException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.QueueException;
import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.HookWrapper;
import bio.terra.stairway.impl.StairwayImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/** Stairway is the object that drives execution of Flights. */
public class Stairway {
  private final StairwayImpl stairwayImpl;

  /**
   * We do initialization in three steps. The constructor does the first step of constructing the
   * object and remembering the inputs. It does not do any flightDao activity. That lets the rest of
   * the application come up and do any database configuration.
   *
   * <p>The second step is the 'initialize' call (below) that performs any necessary database
   * initialization and migration. It sets up the flightDao and returns the current list of Stairway
   * instances recorded in the database.
   *
   * <p>The third step is the 'recover-and-start' call (further below) that performs any requested
   * recovery and opens this Stairway for business. Some flights may already be running depending on
   * how recovery went. They get first crack at the resources. Then submissions from the API and the
   * Work Queue are enabled.
   *
   * @param builder Builder input!
   * @throws StairwayExecutionException on invalid input
   */
  public Stairway(Stairway.Builder builder) throws StairwayExecutionException {
    this.stairwayImpl = new StairwayImpl(this, builder);
  }

  public static Stairway.Builder newBuilder() {
    return new Stairway.Builder();
  }

  /**
   * Second step of initialization
   *
   * @param dataSource database to be used to store Stairway data
   * @param forceCleanStart true will drop any existing stairway data and purge the work queue.
   *     Otherwise existing flights are recovered.
   * @param migrateUpgrade true will run the migrate to upgrade the database
   * @throws DatabaseOperationException failures to perform recovery
   * @throws MigrateException migration failures
   * @throws QueueException queue and queue listener setup
   * @throws StairwayExecutionException another exception
   * @throws InterruptedException on thread shutdown
   * @return list of Stairway instances recorded in the database
   */
  public List<String> initialize(
      DataSource dataSource, boolean forceCleanStart, boolean migrateUpgrade)
      throws DatabaseOperationException, MigrateException, QueueException,
          StairwayExecutionException, InterruptedException {
    return stairwayImpl.initialize(dataSource, forceCleanStart, migrateUpgrade);
  }

  /**
   * Third step of initialization
   *
   * <p>recoverAndStart will do recovery on any obsolete Stairway instances passed in by the caller.
   * Presumably, an edited list of what was returned by the initialize call above.
   *
   * <p>It makes a scan for ready flights and queues them (or launches if no queue).
   *
   * @param obsoleteStairways list of stairways to recover
   * @throws DatabaseSetupException database migrate failure
   * @throws DatabaseOperationException database access failure
   * @throws QueueException queue access failure
   * @throws StairwayExecutionException stairway error
   * @throws InterruptedException interruption during recovery/startup
   */
  public void recoverAndStart(List<String> obsoleteStairways)
      throws DatabaseSetupException, DatabaseOperationException, QueueException,
          InterruptedException, StairwayExecutionException {
    stairwayImpl.recoverAndStart(obsoleteStairways);
  }

  /**
   * Graceful shutdown: instruct stairway to stop executing flights. When running flights hit a step
   * boundary they will yield. No new flights are able to start. Then this thread waits for
   * termination of the thread pool; basically, just exposing the awaitTermination parameters.
   *
   * @param waitTimeout time, in some units to wait before timing out
   * @param unit the time unit of waitTimeout.
   * @return true if we quieted; false if we were interrupted or timed out before quieting down
   */
  public boolean quietDown(long waitTimeout, TimeUnit unit) {
    return stairwayImpl.quietDown(waitTimeout, unit);
  }

  /**
   * Not-so-graceful shutdown: shutdown the pool which will cause an InterruptedException on all of
   * the flights. That _should_ cause the flights to rapidly terminate and get set to unowned.
   *
   * @param waitTimeout time, in some units to wait before timing out
   * @param unit the time unit of waitTimeout.
   * @throws InterruptedException on interruption during termination
   * @return true if the thread pool cleaned up in time; false if it didn't
   */
  public boolean terminate(long waitTimeout, TimeUnit unit) throws InterruptedException {
    return stairwayImpl.terminate(waitTimeout, unit);
  }

  /**
   * Method to generate a flight id. This is a convenience method to allow clients to generate
   * compliant flight ids for Stairway. You don't have to use it.
   *
   * @return 22 character, base64url-encoded UUID
   * @see <a href="https://base64.guru/standards/base64url">Base64 URL</a>
   */
  public String createFlightId() {
    return ShortUUID.get();
  }

  /**
   * Submit a flight for execution.
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @throws DatabaseOperationException failure during flight object creation, persisting to
   *     database or launching
   * @throws StairwayExecutionException failure queuing the flight * @throws
   * @throws DuplicateFlightIdSubmittedException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  public void submit(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {
    stairwayImpl.submit(flightId, flightClass, inputParameters, false, null);
  }

  /**
   * Submit a flight to queue for execution.
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @throws DatabaseOperationException failure during flight object creation, persisting to
   *     database or launching
   * @throws StairwayExecutionException failure queuing the flight
   * @throws DuplicateFlightIdSubmittedException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  public void submitToQueue(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {
    stairwayImpl.submit(flightId, flightClass, inputParameters, true, null);
  }

  public void submitWithDebugInfo(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {
    stairwayImpl.submit(flightId, flightClass, inputParameters, shouldQueue, debugInfo);
  }

  /**
   * Try to resume a flight. If the flight is unowned and either in QUEUED, WAITING or READY state,
   * then this Stairway takes ownership and executes the rest of the flight. There can be race
   * conditions with other Stairway's on resume. It is not an error if the flight is not resumed by
   * this Stairway.
   *
   * @param flightId the flight to try to resume
   * @return true if this Stairway owns and is executing the flight; false if ownership could not be
   *     claimed
   * @throws DatabaseOperationException failure during flight database operations
   * @throws InterruptedException on shutdown during resume
   */
  public boolean resume(String flightId) throws DatabaseOperationException, InterruptedException {
    return stairwayImpl.resume(flightId);
  }

  /**
   * Delete a flight - this removes the database record of a flight. Setting force delete to true
   * allows deleting a flight that is not marked as done. We allow that in case the flight state
   * gets corrupted somehow and needs manual cleanup. If force delete is false, and the flight is
   * not done, we throw.
   *
   * @param flightId flight to delete
   * @param forceDelete boolean to allow force deleting of flight database state, regardless of
   *     flight state
   * @throws DatabaseOperationException errors from removing the flight in memory and in database
   * @throws InterruptedException on shutdown during delete
   */
  public void deleteFlight(String flightId, boolean forceDelete)
      throws DatabaseOperationException, InterruptedException {
    stairwayImpl.deleteFlight(flightId, forceDelete);
  }

  /**
   * Wait for a flight to complete
   *
   * <p>This is a very simple polling method to help you get started with Stairway. It is probably
   * not what you want for production code.
   *
   * @param flightId the flight to wait for
   * @param pollSeconds sleep time for each poll cycle; if null, defaults to 10 seconds
   * @param pollCycles number of times to poll; if null, we poll forever
   * @return flight state object
   * @throws DatabaseOperationException failure to get flight state
   * @throws FlightException if interrupted or polling interval expired
   * @throws InterruptedException on shutdown while waiting for flight completion
   */
  @Deprecated
  public FlightState waitForFlight(String flightId, Integer pollSeconds, Integer pollCycles)
      throws DatabaseOperationException, FlightException, InterruptedException {
    int sleepSeconds = (pollSeconds == null) ? 10 : pollSeconds;
    int pollCount = 0;

    while (pollCycles == null || pollCount < pollCycles) {
      // loop getting flight state and sleeping
      TimeUnit.SECONDS.sleep(sleepSeconds);
      FlightState state = getFlightState(flightId);
      if (!state.isActive()) {
        return state;
      }
      pollCount++;
    }
    throw new FlightException("Flight did not complete in the allowed wait time");
  }

  public Control getControl() {
    return StairwayImpl.getControl();
  }

  /**
   * Get the state of a specific flight If the flight is complete and still in our in-memory map, we
   * remove it. The logic is that if getFlightState is called, then either the wait finished or we
   * are polling and won't perform a wait.
   *
   * @param flightId identifies the flight to retrieve state for
   * @return FlightState state of the flight
   * @throws DatabaseOperationException not found in the database or unexpected database issues
   * @throws InterruptedException on shutdown
   */
  public FlightState getFlightState(String flightId)
      throws DatabaseOperationException, InterruptedException {
    return stairwayImpl.getFlightState(flightId);
  }

  /**
   * Enumerate flights - returns a range of flights ordered by submit time. Note that there can be
   * "jitter" in the paging through flights if new flights are submitted.
   *
   * <p>You can add one or more predicates in a filter list. The filters are logically ANDed
   * together and applied to the input parameters of flights. That lets you add input parameters
   * (like who is running the flight) and then select by that to show flights being run by that
   * user. {@link FlightFilter} documents the different filters and their arguments.
   *
   * <p>The and limit are applied after the filtering is done.
   *
   * @param offset offset of the row ordered by most recent flight first
   * @param limit limit the number of rows returned
   * @param filter predicates to apply to filter flights
   * @return List of FlightState
   * @throws DatabaseOperationException unexpected database errors
   * @throws InterruptedException on shutdown
   */
  public List<FlightState> getFlights(int offset, int limit, FlightFilter filter)
      throws DatabaseOperationException, InterruptedException {
    return stairwayImpl.getFlights(offset, limit, filter);
  }

  /** @return name of this stairway instance */
  public String getStairwayName() {
    return stairwayImpl.getStairwayName();
  }

  public HookWrapper getHookWrapper() {
    return stairwayImpl.getHookWrapper();
  }

  /** @return id of this stairway instance */
  public String getStairwayId() {
    return stairwayImpl.getStairwayId();
  }

  /**
   * Return a list of the names of Stairway instances known to this stairway. They may not all be
   * active.
   *
   * @return List of stairway instance names
   * @throws DatabaseOperationException unexpected database error
   * @throws InterruptedException thread shutdown
   */
  // TODO: move to Control
  public List<String> getStairwayInstanceList()
      throws DatabaseOperationException, InterruptedException {
    return stairwayImpl.getStairwayInstanceList();
  }

  /**
   * Recover any orphaned flights from a particular Stairway instance
   *
   * @param stairwayName name of a stairway instance to recover
   * @throws DatabaseOperationException database access error
   * @throws InterruptedException interruption during recovery
   * @throws StairwayExecutionException stairway error
   */
  // TODO: deprecate - no one uses this; maybe move to control?
  public void recoverStairway(String stairwayName)
      throws DatabaseOperationException, InterruptedException, StairwayExecutionException {
    stairwayImpl.recoverStairway(stairwayName);
  }

  public static class Builder {
    private final List<StairwayHook> stairwayHooks = new ArrayList<>();
    private Integer maxParallelFlights;
    private Integer maxQueuedFlights;
    private Object applicationContext;
    private ExceptionSerializer exceptionSerializer;
    private String stairwayName;
    private FlightFactory flightFactory;
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
    public Builder maxParallelFlights(int maxParallelFlights) {
      this.maxParallelFlights = maxParallelFlights;
      return this;
    }

    public Integer getMaxParallelFlights() {
      return maxParallelFlights;
    }

    /**
     * If we are running a cluster work queue, then this parameter determines the number of flights
     * we allow to queue locally. If the queue depth is, larger than this max, then we stop taking
     * work off of the work queue and we add new submitted flights to the cluster work queue.
     * Default is DEFAULT_MAX_QUEUED_FLIGHTS (1 at this moment)
     *
     * <p>If maxQueuedFlights is smaller than MIN_QUEUED_FLIGHTS (0 at this moment), then it is set
     * to MIN_QUEUED_FLIGHTS. For the admission policy to work properly, MIN_QUEUED_FLIGHT
     * <b>must</b> be greater than 0.
     *
     * @param maxQueuedFlights maximum flights to queue in our local thread pool
     * @return this
     */
    public Builder maxQueuedFlights(int maxQueuedFlights) {
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
    public Builder applicationContext(Object applicationContext) {
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
    public Builder exceptionSerializer(ExceptionSerializer exceptionSerializer) {
      this.exceptionSerializer = exceptionSerializer;
      return this;
    }

    public ExceptionSerializer getExceptionSerializer() {
      return exceptionSerializer;
    }

    /**
     * @param stairwayName Unique name of this Stairway instance. Used to identify which Stairway
     *     owns which flight. Default is "stairway" + random UUID
     * @return this
     */
    public Builder stairwayName(String stairwayName) {
      this.stairwayName = stairwayName;
      return this;
    }

    public String getStairwayName() {
      return stairwayName;
    }

    /**
     * Work queue for this stairway. A Stairway cluster is defined by having the Stairway
     * instances share the same database and the same work queue.
     * @param workQueue is an implementation of the QueueInterface class
     * @return this
     */
    public Builder workQueue(QueueInterface workQueue) {
      this.workQueue = workQueue;
      return this;
    }

    public QueueInterface getWorkQueue() {
      return workQueue;
    }

    /**
     * Flight factory Stairway Flight objects need to be created by class and by name. In all known
     * configurations, Stairway's flight factory can be used. This is an escape hatch in case we run
     * into a configuration that requires special handling for flight creates. Specifying the class
     * loader is simpler and covers the known cases.
     *
     * @param flightFactory flight factory to use
     * @return this
     */
    public Builder flightFactory(FlightFactory flightFactory) {
      this.flightFactory = flightFactory;
      return this;
    }

    public FlightFactory getFlightFactory() {
      return flightFactory;
    }

    /**
     * Each call to stairwayHook adds hook object to a list of hooks. The hooks are processed in the
     * order in which they are added to the builder.
     *
     * @param stairwayHook object containing hooks for logging at beginning and end of flight and
     *     step of stairway
     * @return this
     */
    public Builder stairwayHook(StairwayHook stairwayHook) {
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
    public Builder retentionCheckInterval(Duration retentionCheckInterval) {
      this.retentionCheckInterval = retentionCheckInterval;
      return this;
    }

    public Duration getRetentionCheckInterval() {
      return retentionCheckInterval;
    }

    /**
     * Specify the length of time that completed flights should be retained in the stairway
     * database. Defaults to retaining forever.
     *
     * @param completedFlightRetention duration before clean up
     * @return this
     */
    public Builder completedFlightRetention(Duration completedFlightRetention) {
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
      return new Stairway(this);
    }
  }
}
