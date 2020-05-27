package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.MakeFlightException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.QueueException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stairway is the object that drives execution of Flights. */
public class Stairway {
  private static final Logger logger = LoggerFactory.getLogger(Stairway.class);
  private static final int DEFAULT_MAX_PARALLEL_FLIGHTS = 20;
  private static final int DEFAULT_MAX_QUEUED_FLIGHTS = 10;

  // Constructor parameters
  private final Object applicationContext;
  private final ExceptionSerializer exceptionSerializer;
  private final String stairwayName;
  private final String stairwayClusterName;
  private final String projectId;
  private final int maxParallelFlights;
  private final int maxQueuedFlights;
  private boolean workQueueEnabled;
  private StairwayHook stairwayHook;

  // Initialized state
  private ThreadPoolExecutor threadPool;
  private String stairwayId;
  private AtomicBoolean quietingDown; // flights test this and force yield if true
  private FlightDao flightDao;
  private Queue workQueue;
  private WorkQueueListener workQueueListener;
  private Thread workQueueListenerThread;

  public static class Builder {
    private Integer maxParallelFlights;
    private Integer maxQueuedFlights;
    private Object applicationContext;
    private ExceptionSerializer exceptionSerializer;
    private String stairwayName;
    private String stairwayClusterName;
    private String projectId;
    private StairwayHook stairwayHook;

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

    /**
     * If we are running a cluster work queue, then this parameter determines the number of flights
     * we allow to queue locally. If the queue depth is, larger than this max, then we stop taking
     * work off of the work queue and we add new submitted flights to the cluster work queue.
     * Default is DEFAULT_MAX_QUEUED_FLIGHTS (10 at this moment)
     *
     * @param maxQueuedFlights maximum flights to queue in our local thread pool
     * @return this
     */
    public Builder maxQueuedFlights(int maxQueuedFlights) {
      this.maxQueuedFlights = maxQueuedFlights;
      return this;
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

    /**
     * @param exceptionSerializer Application-specific exception serializer. Default is the Stairway
     *     exception serializer
     * @return this
     */
    public Builder exceptionSerializer(ExceptionSerializer exceptionSerializer) {
      this.exceptionSerializer = exceptionSerializer;
      return this;
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

    /**
     * Unique name of the cluster in which this instance runs. If this Stairway runs in a cluster of
     * Stairway instances, then this must be supplied or they will not work together. This is used
     * to find/create the shared Stairway work queue. Default is "stairwaycluster" + random UUID
     *
     * @param stairwayClusterName Stairway cluster name
     * @return this
     */
    public Builder stairwayClusterName(String stairwayClusterName) {
      this.stairwayClusterName = stairwayClusterName;
      return this;
    }

    /**
     * @param projectId Google projectId to create the Stairway work queue. If not present, no
     *     queuing will be done.
     * @return this
     */
    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * @param stairwayHook object containing hooks for logging at beginning and end of flight and
     *     step of stairway
     * @return this
     */
    public Builder stairwayHook(StairwayHook stairwayHook) {
      this.stairwayHook = stairwayHook;
      return this;
    }

    /**
     * Construct a Stairway instance based on the builder inputs
     *
     * @return Stairway
     */
    public Stairway build() {
      return new Stairway(this);
    }
  }

  public static Stairway.Builder newBuilder() {
    return new Stairway.Builder();
  }

  /**
   * We do initialization in two steps. The constructor does the first step of constructing the
   * object and remembering the inputs. It does not do any flightDao activity. That lets the rest of
   * the application come up and do any database configuration.
   *
   * <p>The second step is the 'initialize' call (below) that sets up the flightDao and performs any
   * recovery needed.
   *
   * @param builder Builder input
   */
  public Stairway(Stairway.Builder builder) {
    this.maxParallelFlights =
        (builder.maxParallelFlights == null)
            ? DEFAULT_MAX_PARALLEL_FLIGHTS
            : builder.maxParallelFlights;

    this.maxQueuedFlights =
        (builder.maxQueuedFlights == null) ? DEFAULT_MAX_QUEUED_FLIGHTS : builder.maxQueuedFlights;

    this.exceptionSerializer =
        (builder.exceptionSerializer == null)
            ? new DefaultExceptionSerializer()
            : builder.exceptionSerializer;

    this.stairwayName =
        (builder.stairwayName == null)
            ? "stairway" + UUID.randomUUID().toString()
            : builder.stairwayName;

    this.stairwayClusterName =
        (builder.stairwayClusterName == null)
            ? "stairwaycluster" + UUID.randomUUID().toString()
            : builder.stairwayClusterName;

    this.applicationContext = builder.applicationContext;
    this.projectId = builder.projectId;
    this.workQueueEnabled = (projectId != null);
    this.quietingDown = new AtomicBoolean();
    this.stairwayHook =
        (builder.stairwayHook == null) ? populateDefaultStairwayHook() : builder.stairwayHook;
  }

  public StairwayHook populateDefaultStairwayHook() {
    return new StairwayHook() {
      @Override
      public HookAction startFlight(FlightContext context) {
        return handleDeafultHook();
      }

      @Override
      public HookAction startStep(FlightContext context) {
        return handleDeafultHook();
      }

      @Override
      public HookAction endFlight(FlightContext context) {
        return handleDeafultHook();
      }

      @Override
      public HookAction endStep(FlightContext context) {
        return handleDeafultHook();
      }
    };
  }

  private HookAction handleDeafultHook() {
    logger.info("Stairway Hook not defined.");
    return HookAction.CONTINUE;
  }

  /**
   * Second step of initialization
   *
   * @param dataSource database to be used to store Stairway data
   * @param forceCleanStart true will drop any existing stairway data and purge the work queue.
   *     Otherwise existing flights are recovered.
   * @param migrateUpgrade true will run the migrate to upgrade the database
   * @throws DatabaseSetupException failed to clean the database on startup
   * @throws DatabaseOperationException failures to perform recovery
   * @throws MigrateException migration failures
   * @throws InterruptedException on shutdown during recovery
   * @throws QueueException queue and queue listener setup
   */
  public void initialize(DataSource dataSource, boolean forceCleanStart, boolean migrateUpgrade)
      throws DatabaseSetupException, DatabaseOperationException, MigrateException, QueueException,
          InterruptedException {

    // Clear quietingDown on initialization. In production, we expect that once we are quieted
    // the process is shut down. However, if the Stairway object is reused, like in unit tests,
    // then we need to clear the flag so we can actually process.
    quietingDown.set(false);

    if (migrateUpgrade) {
      Migrate migrate = new Migrate();
      migrate.initialize("stairway/db/changelog.xml", dataSource);
    }

    this.flightDao = new FlightDao(dataSource, exceptionSerializer);

    createThreadPool();
    setupWorkQueue();

    if (forceCleanStart) {
      // If cleaning, set the stairway id after cleaning (or it gets cleaned!)
      flightDao.startClean();
      stairwayId = flightDao.findOrCreateStairwayInstance(stairwayName);
      if (workQueue != null) {
        workQueue.purgeQueue();
      }
    } else {
      stairwayId = flightDao.findOrCreateStairwayInstance(stairwayName);
      recoverFlights();
    }
    startWorkQueueListener();
  }

  private void createThreadPool() {
    threadPool =
        new ThreadPoolExecutor(
            maxParallelFlights,
            maxParallelFlights,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
  }

  private void setupWorkQueue() throws QueueException {
    if (!workQueueEnabled) {
      return; // no work queue for you!
    }

    String topicId = stairwayClusterName + "-workqueue";
    String subscriptionId = stairwayClusterName + "-workqueue-sub";

    try {
      workQueue = new Queue(this, projectId, topicId, subscriptionId);
    } catch (IOException ex) {
      throw new QueueException("Failed to create work queue", ex);
    }
  }

  private void startWorkQueueListener() throws QueueException {
    if (!workQueueEnabled) {
      return;
    }
    workQueueListener = new WorkQueueListener(this, maxQueuedFlights, workQueue);
    workQueueListenerThread = new Thread(workQueueListener);
    workQueueListenerThread.start();
  }

  private void terminateWorkQueueListener(long workQueueWaitSeconds) {
    if (!workQueueEnabled) {
      return;
    }
    // workQueueListener will notice quietingDown and stop, but it is often
    // waiting in sleep or in pull from queue and needs to be interrupted to terminate.
    workQueueListenerThread.interrupt();
    try {
      workQueueListenerThread.join(TimeUnit.SECONDS.toMillis(workQueueWaitSeconds));
    } catch (InterruptedException ex) {
      logger.info("Failed to join work queue listener thread");
    }
    logger.info("Shutdown work queue listener");
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
    long waitSeconds = unit.toSeconds(waitTimeout);
    long workQueueWaitSeconds;
    if (waitSeconds < 30) {
      logger.warn("Wait time is less than 30 seconds.");
      workQueueWaitSeconds = 1;
    } else {
      workQueueWaitSeconds = 5;
    }
    long threadPoolWaitSeconds = waitTimeout - workQueueWaitSeconds;

    quietingDown.set(true);
    terminateWorkQueueListener(workQueueWaitSeconds);
    threadPool.shutdown();
    try {
      return threadPool.awaitTermination(threadPoolWaitSeconds, unit);
    } catch (InterruptedException ex) {
      return false;
    }
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
    terminateWorkQueueListener(0);
    List<Runnable> neverStartedFlights = threadPool.shutdownNow();
    for (Runnable flightRunnable : neverStartedFlights) {
      Flight flight = (Flight) flightRunnable;
      try {
        logger.info("Requeue never-started flight: " + flight.context().flightDesc());
        flight.context().setFlightStatus(FlightStatus.READY);
        flightDao.exit(flight.context());
      } catch (DatabaseOperationException | FlightException ex) {
        // Not much to do on termination
        logger.warn("Unable to requeue never-started flight: " + flight.context().flightDesc(), ex);
      }
    }
    return threadPool.awaitTermination(waitTimeout, unit);
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
   * @throws StairwayExecutionException failure queuing the flight
   * @throws InterruptedException this thread was interrupted
   */
  public void submit(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {
    submitWorker(flightId, flightClass, inputParameters, false);
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
   * @throws InterruptedException this thread was interrupted
   */
  public void submitToQueue(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {
    submitWorker(flightId, flightClass, inputParameters, true);
  }

  private void submitWorker(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {

    if (isQuietingDown()) {
      throw new MakeFlightException("Stairway is shutting down and cannot accept a new flight");
    }

    if (flightClass == null || inputParameters == null) {
      throw new MakeFlightException(
          "Must supply non-null flightClass and inputParameters to submit");
    }
    Flight flight = makeFlight(flightClass, inputParameters);
    FlightContext context = flight.context();
    context.setFlightId(flightId);

    // If we are submitting, but our local thread pool is too backed up, send the flight to the
    // queue.
    if (!shouldQueue && (threadPool.getQueue().size() >= maxQueuedFlights)) {
      shouldQueue = true;
      logger.info("Local thread pool queue is too deep. Submitting flight to queue: " + flightId);
    }

    if (workQueueEnabled && shouldQueue) {
      // Submit to the queue
      context.setFlightStatus(FlightStatus.READY);
      flightDao.submit(context);
      queueFlight(context);
    } else {
      // Submit directly
      context.setStairway(this);
      flightDao.submit(context);
      launchFlight(flight);
    }
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
    if (isQuietingDown()) {
      throw new MakeFlightException("Stairway is shutting down and cannot resume a flight");
    }

    FlightContext flightContext = flightDao.resume(stairwayId, flightId);
    if (flightContext == null) {
      return false;
    }

    resumeOneFlight(flightContext);
    return true;
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

    if (!forceDelete) {
      FlightState state = flightDao.getFlightState(flightId);
      if (state.getFlightStatus() == FlightStatus.RUNNING) {
        throw new DatabaseOperationException("Cannot delete an active flight");
      }
    }

    flightDao.delete(flightId);
  }

  /**
   * Wait for a flight to complete
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
    return flightDao.getFlightState(flightId);
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
    return flightDao.getFlights(offset, limit, filter);
  }

  /** @return name of this stairway instance */
  public String getStairwayName() {
    return stairwayName;
  }

  public StairwayHook getStairwayHook() {
    return stairwayHook;
  }

  /** @return id of this stairway instance */
  public String getStairwayId() {
    return stairwayId;
  }

  boolean isQuietingDown() {
    return quietingDown.get();
  }

  // Exposed for unit testing
  FlightDao getFlightDao() {
    return flightDao;
  }

  // Exposed for work queue listener
  ThreadPoolExecutor getThreadPool() {
    return threadPool;
  }

  void exitFlight(FlightContext context)
      throws DatabaseOperationException, FlightException, StairwayExecutionException,
          InterruptedException {
    // save the flight state in the database
    flightDao.exit(context);

    if (context.getFlightStatus() == FlightStatus.READY && workQueueEnabled) {
      queueFlight(context);
    }
  }

  private void queueFlight(FlightContext context)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {
    // If the flight state is READY, then we put the flight on the queue and mark it queued in the
    // database. We cannot go directly from RUNNING to QUEUED. Suppose we put the flight in
    // the queue before it was marked QUEUED. Another Stairway instance would see it was
    // RUNNING and decide it shouldn't run it. Suppose we put the flight in the queue after
    // we marked it QUEUED. Then if we fail before we put it on the queue, no one knows it should
    // be queued. To close that window, we use the READY state to decouple the flight from RUNNING.
    // Then we queue, then we mark as queued. Another Stairway looking for orphans, will find the
    // READY
    // and queue it. Putting a flight on the queue twice is not a problem. Stairway instances race
    // to
    // see who gets to run it.
    String message = QueueMessage.serialize(new QueueMessageReady(context.getFlightId()));
    workQueue.queueMessage(message);
    context.setFlightStatus(FlightStatus.QUEUED);
    flightDao.queued(context);
  }

  /**
   * Find any incomplete flights and recover them. We overwrite the flight context of this flight
   * with the recovered flight context. The normal constructor path needs to give the input
   * parameters to the flight subclass. This is a case where we don't really want to have the Flight
   * object set up its own context. It is simpler to override it than to make a separate code path
   * for this recovery case.
   *
   * <p>The flightlog records the last operation performed; so we need to set the execution point to
   * the next step index.
   *
   * @throws DatabaseOperationException on database errors
   */
  private void recoverFlights() throws DatabaseOperationException, InterruptedException {
    List<FlightContext> flightList = flightDao.recover(stairwayId);
    for (FlightContext flightContext : flightList) {
      resumeOneFlight(flightContext);
    }
  }

  private void resumeOneFlight(FlightContext flightContext) {
    Flight flight =
        makeFlightFromName(flightContext.getFlightClassName(), flightContext.getInputParameters());
    flightContext.setStairway(this);
    flightContext.nextStepIndex(); // position the flight to execute the next thing
    flight.setFlightContext(flightContext);
    launchFlight(flight);
  }

  /*
   * Build the task context to keep track of the running flight.
   * Once it is launched, hook it into the {@link #taskContextMap} so other
   * calls can resolve it.
   */
  private void launchFlight(Flight flight) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Stairway thread pool: "
              + threadPool.getActiveCount()
              + " active from pool of "
              + threadPool.getPoolSize());
    }
    logger.info("Launching flight " + flight.context().flightDesc());
    threadPool.submit(flight);
  }

  /*
   * Create a Flight instance given the class name of the derived class of Flight
   * and the input parameters.
   *
   * Note that you can adjust the steps you generate based on the input parameters.
   */
  private Flight makeFlight(Class<? extends Flight> flightClass, FlightMap inputParameters) {
    try {
      // Find the flightClass constructor that takes the input parameter map and
      // use it to make the flight.
      Constructor constructor = flightClass.getConstructor(FlightMap.class, Object.class);
      Flight flight = (Flight) constructor.newInstance(inputParameters, applicationContext);
      return flight;
    } catch (InvocationTargetException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException ex) {
      throw new MakeFlightException("Failed to make a flight from class '" + flightClass + "'", ex);
    }
  }

  /*
   * Version of makeFlight that accepts the class name instead of the class object
   * We use the class name to store and retrieve from the flightDao when we recover.
   */
  private Flight makeFlightFromName(String className, FlightMap inputMap) {
    try {
      Class<?> someClass = Class.forName(className);
      if (Flight.class.isAssignableFrom(someClass)) {
        Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
        return makeFlight(flightClass, inputMap);
      }
      // Error case
      throw new MakeFlightException(
          "Failed to make a flight from class name '"
              + className
              + "' - it is not a subclass of Flight");

    } catch (ClassNotFoundException ex) {
      throw new MakeFlightException(
          "Failed to make a flight from class name '" + className + "'", ex);
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("threadPool", threadPool)
        .append("flightDao", flightDao)
        .append("applicationContext", applicationContext)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    Stairway stairway = (Stairway) o;

    return new EqualsBuilder()
        .append(logger, stairway.logger)
        .append(threadPool, stairway.threadPool)
        .append(flightDao, stairway.flightDao)
        .append(applicationContext, stairway.applicationContext)
        .append(exceptionSerializer, stairway.exceptionSerializer)
        .append(stairwayName, stairway.stairwayName)
        .append(stairwayId, stairway.stairwayId)
        .append(quietingDown, stairway.quietingDown)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(logger)
        .append(threadPool)
        .append(flightDao)
        .append(applicationContext)
        .append(exceptionSerializer)
        .append(stairwayName)
        .append(stairwayId)
        .append(quietingDown)
        .toHashCode();
  }
}
