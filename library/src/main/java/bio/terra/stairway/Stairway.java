package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.DuplicateFlightIdSubmittedException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.MakeFlightException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.QueueException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
  private static final int DEFAULT_MAX_QUEUED_FLIGHTS = 2;
  private static final int MIN_QUEUED_FLIGHTS = 0;
  private static final int SCHEDULED_POOL_CORE_THREADS = 5;

  // Constructor parameters
  private final Object applicationContext;
  private final ExceptionSerializer exceptionSerializer;
  private final String stairwayName;
  private final String workQueueProjectId;
  private final String workQueueTopicId;
  private final String workQueueSubscriptionId;
  private final boolean workQueueCreate;
  private final int maxParallelFlights;
  private final int maxQueuedFlights;
  private final boolean workQueueEnabled;
  private final HookWrapper hookWrapper;
  private final FlightFactory flightFactory;
  private final Duration retentionCheckInterval;
  private final Duration completedFlightRetention;
  private final AtomicBoolean quietingDown;

  // Initialized state
  private StairwayThreadPool threadPool;
  private String stairwayId;
  private StairwayInstanceDao stairwayInstanceDao;
  private FlightDao flightDao;
  private Queue workQueue;
  private Thread workQueueListenerThread;
  private ScheduledExecutorService scheduledPool;
  private Control control;

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
    this.maxParallelFlights =
        (builder.maxParallelFlights == null)
            ? DEFAULT_MAX_PARALLEL_FLIGHTS
            : builder.maxParallelFlights;

    this.maxQueuedFlights =
        (builder.maxQueuedFlights == null)
            ? DEFAULT_MAX_QUEUED_FLIGHTS
            : (builder.maxQueuedFlights < MIN_QUEUED_FLIGHTS
                ? MIN_QUEUED_FLIGHTS
                : builder.maxQueuedFlights);

    this.exceptionSerializer =
        (builder.exceptionSerializer == null)
            ? new DefaultExceptionSerializer()
            : builder.exceptionSerializer;

    this.stairwayName =
        (builder.stairwayName == null)
            ? "stairway" + UUID.randomUUID().toString()
            : builder.stairwayName;

    String stairwayClusterName =
        (builder.stairwayClusterName == null)
            ? "stairwaycluster" + UUID.randomUUID().toString()
            : builder.stairwayClusterName;

    this.flightFactory =
        (builder.flightFactory == null) ? new StairwayFlightFactory() : builder.flightFactory;

    this.workQueueProjectId = builder.workQueueProjectId;
    this.workQueueEnabled = builder.enableWorkQueue;
    if (workQueueProjectId == null && workQueueEnabled) {
      throw new StairwayExecutionException(
          "project id must be specified if it enable work queue is requested");
    }
    if (!workQueueEnabled) {
      // work queue is not requested so we do not create one
      workQueueCreate = false;
      this.workQueueTopicId = null;
      this.workQueueSubscriptionId = null;
    } else {
      // work queue is requested. if we have to create it we build name based on the
      // cluster name.
      workQueueCreate = (builder.workQueueTopicId == null);
      if (workQueueCreate) {
        this.workQueueTopicId = stairwayClusterName + "-workqueue";
        this.workQueueSubscriptionId = stairwayClusterName + "-workqueue-sub";
      } else {
        this.workQueueTopicId = builder.workQueueTopicId;
        this.workQueueSubscriptionId = builder.workQueueSubscriptionId;
        if (workQueueSubscriptionId == null) {
          throw new StairwayExecutionException(
              "subscription id must be specified if topic id is specified");
        }
      }
    }

    this.applicationContext = builder.applicationContext;
    this.quietingDown = new AtomicBoolean();
    this.hookWrapper = new HookWrapper(builder.stairwayHooks);
    this.completedFlightRetention = builder.completedFlightRetention;
    this.retentionCheckInterval =
        (builder.retentionCheckInterval == null)
            ? Duration.ofDays(1)
            : builder.retentionCheckInterval;
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

    // If we have been shut down, do not restart.
    if (isQuietingDown()) {
      throw new StairwayExecutionException("Stairway is shut down and cannot be initialized");
    }

    stairwayInstanceDao = new StairwayInstanceDao(dataSource);
    flightDao = new FlightDao(dataSource, stairwayInstanceDao, exceptionSerializer, hookWrapper);
    control = new Control(this, dataSource, flightDao, stairwayInstanceDao);

    if (forceCleanStart) {
      // Drop all tables and recreate the database
      Migrate migrate = new Migrate();
      migrate.initialize("stairway/db/changelog.xml", dataSource);
    } else if (migrateUpgrade) {
      // Migrate the database to a revised schema, if needed
      Migrate migrate = new Migrate();
      migrate.upgrade("stairway/db/changelog.xml", dataSource);
    }

    configureThreadPools();
    setupWorkQueue(forceCleanStart);
    return stairwayInstanceDao.getList();
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

    if (obsoleteStairways != null) {
      for (String instance : obsoleteStairways) {
        String stairwayId = stairwayInstanceDao.lookupId(instance);
        logger.info("Recovering stairway " + instance);
        flightDao.disownRecovery(stairwayId);
      }
    }

    // Start this Stairway instance up!
    stairwayId = stairwayInstanceDao.findOrCreate(stairwayName);

    // Recover any flights in the READY state
    recoverReady();
    startWorkQueueListener();
  }

  private void configureThreadPools() {
    threadPool =
        new StairwayThreadPool(
            maxParallelFlights,
            maxParallelFlights,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    scheduledPool = new ScheduledThreadPoolExecutor(SCHEDULED_POOL_CORE_THREADS);
    // If we have retention settings then set up the regular flight cleaner
    if (retentionCheckInterval != null && completedFlightRetention != null) {
      scheduledPool.scheduleWithFixedDelay(
          new CompletedFlightCleaner(completedFlightRetention, flightDao),
          retentionCheckInterval.toSeconds(),
          retentionCheckInterval.toSeconds(),
          TimeUnit.SECONDS);
    }
  }

  private void setupWorkQueue(boolean forceClean) throws QueueException {
    if (!workQueueEnabled) {
      return; // no work queue for you!
    }

    try {
      if (workQueueCreate) {
        QueueCreate.makeTopic(workQueueProjectId, workQueueTopicId);
        QueueCreate.makeSubscription(workQueueProjectId, workQueueTopicId, workQueueSubscriptionId);
      }

      workQueue = new Queue(this, workQueueProjectId, workQueueTopicId, workQueueSubscriptionId);
      if (forceClean) {
        workQueue.purgeQueue();
      }
    } catch (IOException ex) {
      throw new QueueException("Failed to create work queue", ex);
    }
  }

  private void startWorkQueueListener() throws QueueException {
    if (!workQueueEnabled) {
      return;
    }
    WorkQueueListener workQueueListener = new WorkQueueListener(this, workQueue);
    workQueueListenerThread = new Thread(workQueueListener);
    workQueueListenerThread.start();
  }

  private void terminateWorkQueueListener(long workQueueWaitSeconds) {
    if (!workQueueEnabled) {
      return;
    }
    logger.info("Shutting down work queue listener");
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
    quietingDown.set(true);
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
   * @throws StairwayExecutionException failure queuing the flight * @throws
   * @throws DuplicateFlightIdSubmittedException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  public void submit(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {
    submitWorker(flightId, flightClass, inputParameters, false, null);
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
    submitWorker(flightId, flightClass, inputParameters, true, null);
  }

  public void submitWithDebugInfo(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {
    submitWorker(flightId, flightClass, inputParameters, shouldQueue, debugInfo);
  }

  private void submitWorker(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdSubmittedException {

    if (flightClass == null || inputParameters == null) {
      throw new MakeFlightException(
          "Must supply non-null flightClass and inputParameters to submit");
    }
    Flight flight =
        flightFactory.makeFlight(flightClass, inputParameters, applicationContext, debugInfo);
    FlightContext context = flight.context();
    context.setFlightId(flightId);

    if (isQuietingDown()) {
      shouldQueue = true;
      logger.info("Shutting down. Submitting flight to queue: " + flightId);
    }

    // If we are submitting, but we don't have space available for the flight, queue it
    if (!shouldQueue && !spaceAvailable()) {
      shouldQueue = true;
      logger.info("Local thread pool queue is too deep. Submitting flight to queue: " + flightId);
    }

    if (workQueueEnabled && shouldQueue) {
      // Submit to the queue
      context.setFlightStatus(FlightStatus.READY);
      flightDao.submit(context);
      queueFlight(context);
    } else {
      // Submit directly - not allowed if we are shutting down
      if (isQuietingDown()) {
        throw new MakeFlightException("Stairway is shutting down and cannot accept a new flight");
      }
      context.setStairway(this);
      try {
        flightDao.submit(context);
      } catch (DuplicateFlightIdException ex) {
        // Convert the internal runtime exception to a checked exception for clients
        throw new DuplicateFlightIdSubmittedException(ex.getMessage(), ex);
      }
      launchFlight(flight);
    }
  }

  /**
   * This code trinket is used by submit and WorkQueueListener to decide if there is room
   *
   * @return true if there is room to take on more work.
   */
  boolean spaceAvailable() {
    logger.debug(
        "Space available? active: "
            + threadPool.getActiveFlights()
            + " of max: "
            + maxParallelFlights
            + " queueSize: "
            + threadPool.getQueuedFlights()
            + " of max: "
            + maxQueuedFlights);
    return ((threadPool.getActiveFlights() < maxParallelFlights)
        || (threadPool.getQueuedFlights() < maxQueuedFlights));
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

    Flight flight =
        flightFactory.makeFlightFromName(
            flightContext.getFlightClassName(),
            flightContext.getInputParameters(),
            applicationContext,
            flightContext.getDebugInfo());
    flightContext.setStairway(this);
    flight.setFlightContext(flightContext);
    launchFlight(flight);

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
    return control;
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

  public HookWrapper getHookWrapper() {
    return hookWrapper;
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
    if (context.getFlightStatus() == FlightStatus.READY_TO_RESTART) {
      if (workQueueEnabled) {
        queueFlight(context);
      } else {
        resume(context.getFlightId());
      }
    }
  }

  private void queueFlight(FlightContext flightContext)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {
    // If the flight state is READY, then we put the flight on the queue and mark it queued in the
    // database. We cannot go directly from RUNNING to QUEUED. Suppose we put the flight in
    // the queue before it was marked QUEUED. Another Stairway instance would see it was
    // RUNNING and decide it shouldn't run it. Suppose we put the flight in the queue after
    // we marked it QUEUED. Then if we fail before we put it on the queue, no one knows it should
    // be queued. To close that window, we use the READY state to decouple the flight from RUNNING.
    // Then we queue, then we mark as queued. Another Stairway looking for orphans, will find the
    // READY and queue it. Putting a flight on the queue twice is not a problem. Stairway
    // instances race to see who gets to run it.
    String message = QueueMessage.serialize(new QueueMessageReady(flightContext.getFlightId()));
    workQueue.queueMessage(message);
    flightDao.queued(flightContext);
  }

  /**
   * Return a list of the names of Stairway instances known to this stairway. They may not all be
   * active.
   *
   * @return List of stairway instance names
   * @throws DatabaseOperationException unexpected database error
   * @throws InterruptedException thread shutdown
   */
  public List<String> getStairwayInstanceList()
      throws DatabaseOperationException, InterruptedException {
    return stairwayInstanceDao.getList();
  }

  /**
   * Recover any orphaned flights from a particular Stairway instance
   *
   * @param stairwayName name of a stairway instance to recover
   * @throws DatabaseOperationException database access error
   * @throws InterruptedException interruption during recovery
   * @throws StairwayExecutionException stairway error
   */
  public void recoverStairway(String stairwayName)
      throws DatabaseOperationException, InterruptedException, StairwayExecutionException {
    String stairwayId = stairwayInstanceDao.lookupId(stairwayName);
    flightDao.disownRecovery(stairwayId);
    recoverReady();
  }

  /**
   * Recover flights in the READY state. Flights need recovery from the READY state in two cases:
   *
   * <ol>
   *   <li>when a Stairway instance fails during the process of putting a flight in the work queue
   *   <li>the second step of recovery of a failed Stairway instance; in the first step, we disowned
   *       the running flights and put them in the READY state.
   * </ol>
   *
   * If the work queue is enabled, we then queue the flights. We don't want this Stairway instance
   * to take on all of the recovered flights itself. If there is no workQueue, we build the flight
   * object and launch it ourselves.
   *
   * @throws DatabaseOperationException unexpected database error
   * @throws InterruptedException on shutdown during recovery
   * @throws StairwayExecutionException stairway error
   */
  void recoverReady()
      throws DatabaseOperationException, InterruptedException, StairwayExecutionException {
    List<String> readyFlightList = flightDao.getReadyFlights();
    for (String flightId : readyFlightList) {
      if (workQueueEnabled) {
        FlightContext flightContext = flightDao.makeFlightContextById(flightId);
        queueFlight(flightContext);
      } else {
        resume(flightId);
      }
    }
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

  public static class Builder {
    private final List<StairwayHook> stairwayHooks = new ArrayList<>();
    private Integer maxParallelFlights;
    private Integer maxQueuedFlights;
    private Object applicationContext;
    private ExceptionSerializer exceptionSerializer;
    private String stairwayName;
    private String stairwayClusterName;
    private boolean enableWorkQueue;
    private FlightFactory flightFactory;
    private String workQueueProjectId;
    private String workQueueTopicId;
    private String workQueueSubscriptionId;
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
     * You must explicitly enable the work queue when running in a cluster configuration. That
     * allows for testing where a work queue is not used or created.
     *
     * @param enableWorkQueue said true to enable the work queue
     * @return this
     */
    public Builder enableWorkQueue(boolean enableWorkQueue) {
      this.enableWorkQueue = enableWorkQueue;
      return this;
    }

    /**
     * Use the retention controls instead of this method. It is a no-op.
     *
     * @param keepFlightLog ignored
     * @return this
     */
    @Deprecated
    public Builder keepFlightLog(boolean keepFlightLog) {
      return this;
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

    /**
     * Specify the project id in which to find/create the work queue topic and subscription If not
     * supplied, no queue will be used.
     *
     * @param workQueueProjectId a goggle project id
     * @return this
     */
    public Builder workQueueProjectId(String workQueueProjectId) {
      this.workQueueProjectId = workQueueProjectId;
      return this;
    }

    /**
     * Specify the topic id of the work queue. if no id is supplied, then a topic and subscription
     * will be created based on the cluster name.
     *
     * @param workQueueTopicId the name of the topic to use in the work queue project
     * @return this
     */
    public Builder workQueueTopicId(String workQueueTopicId) {
      this.workQueueTopicId = workQueueTopicId;
      return this;
    }

    /**
     * Specify these subscription id for the work queue. If the topic id is not supplied, then this
     * parameter is ignored. If the topic id is supplied, then this parameter it must be supplied.
     *
     * @param workQueueSubscriptionId the name of the subscription to use in the work queue project
     * @return this
     */
    public Builder workQueueSubscriptionId(String workQueueSubscriptionId) {
      this.workQueueSubscriptionId = workQueueSubscriptionId;
      return this;
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
