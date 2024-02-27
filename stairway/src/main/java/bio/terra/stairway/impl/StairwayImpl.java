package bio.terra.stairway.impl;

import bio.terra.stairway.Control;
import bio.terra.stairway.ExceptionSerializer;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightEnumeration;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.ShortUUID;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StairwayBuilder;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.FlightWaitTimedOutException;
import bio.terra.stairway.exception.MakeFlightException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.exception.StairwayShutdownException;
import bio.terra.stairway.queue.WorkQueueManager;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

/**
 * StairwayImpl holds the implementation of the Stairway library. This class is not intended for
 * direct use by clients.
 */
public class StairwayImpl implements Stairway {
  private static final Logger logger = LoggerFactory.getLogger(StairwayImpl.class);
  private static final int DEFAULT_MAX_PARALLEL_FLIGHTS = 20;
  private static final int DEFAULT_MAX_QUEUED_FLIGHTS = 2;
  private static final int MIN_QUEUED_FLIGHTS = 0;
  private static final int SCHEDULED_POOL_CORE_THREADS = 5;

  // Constructor parameters
  private final Object applicationContext;
  private final ExceptionSerializer exceptionSerializer;
  private final String stairwayName; // always identical to stairwayId
  private final int maxParallelFlights;
  private final int maxQueuedFlights;
  private final WorkQueueManager queueManager;
  private final HookWrapper hookWrapper;
  private final Duration retentionCheckInterval;
  private final Duration completedFlightRetention;
  private final AtomicBoolean quietingDown;

  // Initialized state
  private StairwayThreadPool threadPool;
  private StairwayInstanceDao stairwayInstanceDao;
  private FlightDao flightDao;
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
   * @param builder Builder input
   * @throws StairwayExecutionException on invalid input
   */
  public StairwayImpl(StairwayBuilder builder) throws StairwayExecutionException {
    this.maxParallelFlights =
        (builder.getMaxParallelFlights() == null)
            ? DEFAULT_MAX_PARALLEL_FLIGHTS
            : builder.getMaxParallelFlights();

    this.maxQueuedFlights =
        (builder.getMaxQueuedFlights() == null)
            ? DEFAULT_MAX_QUEUED_FLIGHTS
            : (builder.getMaxQueuedFlights() < MIN_QUEUED_FLIGHTS
                ? MIN_QUEUED_FLIGHTS
                : builder.getMaxQueuedFlights());

    this.exceptionSerializer =
        (builder.getExceptionSerializer() == null)
            ? new DefaultExceptionSerializer()
            : builder.getExceptionSerializer();

    this.stairwayName =
        (builder.getStairwayName() == null)
            ? "stairway" + UUID.randomUUID().toString()
            : builder.getStairwayName();

    this.queueManager = new WorkQueueManager(this, builder.getWorkQueue());

    this.applicationContext = builder.getApplicationContext();
    this.quietingDown = new AtomicBoolean();
    this.hookWrapper = new HookWrapper(builder.getStairwayHooks());
    this.completedFlightRetention = builder.getCompletedFlightRetention();
    this.retentionCheckInterval =
        (builder.getRetentionCheckInterval() == null)
            ? Duration.ofDays(1)
            : builder.getRetentionCheckInterval();
  }

  /**
   * Second step of initialization
   *
   * @param dataSource database to be used to store Stairway data
   * @param forceCleanStart true will drop any existing stairway data and purge the work queue.
   *     Otherwise existing flights are recovered.
   * @param migrateUpgrade true will run the migrate to upgrade the database
   * @throws StairwayShutdownException stairway is shutdown and cannot initialize
   * @throws DatabaseOperationException failures to perform recovery
   * @throws MigrateException migration failures
   * @throws StairwayException other Stairway exceptions
   * @throws InterruptedException on thread shutdown
   * @return list of Stairway instances recorded in the database
   */
  public List<String> initialize(
      DataSource dataSource, boolean forceCleanStart, boolean migrateUpgrade)
      throws StairwayShutdownException, DatabaseOperationException, MigrateException,
          StairwayException, InterruptedException {

    // If we have been shut down, do not restart.
    if (isQuietingDown()) {
      throw new StairwayShutdownException("Stairway is shut down and cannot be initialized");
    }

    stairwayInstanceDao = new StairwayInstanceDao(dataSource);
    flightDao =
        new FlightDao(
            dataSource, stairwayInstanceDao, exceptionSerializer, hookWrapper, stairwayName);
    control = new ControlImpl(dataSource, flightDao, stairwayInstanceDao);

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
    queueManager.initialize(forceCleanStart);
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
   * @throws DatabaseOperationException database access failure
   * @throws StairwayExecutionException stairway error
   * @throws InterruptedException interruption during recovery/startup
   */
  public void recoverAndStart(List<String> obsoleteStairways)
      throws StairwayException, DatabaseOperationException, InterruptedException,
          StairwayExecutionException {

    if (obsoleteStairways != null) {
      for (String instance : obsoleteStairways) {
        String stairwayId = stairwayInstanceDao.lookupId(instance);
        logger.info("Recovering stairway " + instance);
        flightDao.disownRecovery(stairwayId);
      }
    }

    // Start this Stairway instance up!
    stairwayInstanceDao.findOrCreate(stairwayName);

    // Recover any flights in the READY state
    recoverReady();
    queueManager.start();
  }

  /**
   * Recover any orphaned flights from a particular Stairway instance. This method can be called
   * when a server using Stairway discovers that another Stairway instance has failed. For example,
   * when a Kubernetes listener notices a pod failure.
   *
   * @param stairwayName name of a stairway instance to recover
   * @throws InterruptedException interruption during recovery
   */
  public void recoverStairway(String stairwayName) throws InterruptedException {
    String stairwayId = stairwayInstanceDao.lookupId(stairwayName);
    flightDao.disownRecovery(stairwayId);
    recoverReady();
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
    queueManager.shutdown(workQueueWaitSeconds);
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
   * @throws StairwayException other Stairway error
   * @throws InterruptedException on interruption during termination
   * @return true if the thread pool cleaned up in time; false if it didn't
   */
  public boolean terminate(long waitTimeout, TimeUnit unit)
      throws StairwayException, InterruptedException {
    quietingDown.set(true);
    queueManager.shutdownNow();
    List<Runnable> neverStartedFlights = threadPool.shutdownNow();
    for (Runnable flightRunnable : neverStartedFlights) {
      FlightRunner flightRunner = (FlightRunner) flightRunnable;
      FlightContextImpl flightContext = flightRunner.getFlightContext();
      String flightDesc = flightContext.flightDesc();
      try {
        logger.info("Requeue never-started flight: " + flightDesc);
        flightContext.setFlightStatus(FlightStatus.READY);
        flightDao.exit(flightContext);
      } catch (DatabaseOperationException | StairwayExecutionException ex) {
        // Not much to do on termination
        logger.warn("Unable to requeue never-started flight: " + flightDesc, ex);
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
   * Submit a flight for execution specifing all of the parameters
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @param shouldQueue true to put this flight on the queue; false to try to run it on this
   *     instance
   * @param debugInfo optional debug info structure to inject failures at points in the flight
   * @throws StairwayException failure during flight object creation, persisting to database or
   *     launching
   * @throws StairwayExecutionException failure queuing the flight * @throws
   * @throws DuplicateFlightIdException provided flight id already exists
   * @throws MakeFlightException unable to make a flight object with the provided class
   * @throws InterruptedException this thread was interrupted
   */
  public void submit(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws StairwayException, StairwayExecutionException, InterruptedException,
          DuplicateFlightIdException, MakeFlightException {
    if (flightClass == null || inputParameters == null) {
      throw new MakeFlightException(
          "Must supply non-null flightClass and inputParameters to submit");
    }
    Flight flight = FlightFactory.makeFlight(flightClass, inputParameters, applicationContext);
    FlightContextImpl context = new FlightContextImpl(this, flight, flightId, debugInfo);

    if (isQuietingDown()) {
      shouldQueue = true;
      logger.info("Shutting down. Submitting flight to queue: " + flightId);
    }

    // If we are submitting, but we don't have space available for the flight, queue it
    if (!shouldQueue && !spaceAvailable()) {
      shouldQueue = true;
      logger.info("Local thread pool queue is too deep. Submitting flight to queue: " + flightId);
    }

    if (queueManager.isWorkQueueEnabled() && shouldQueue) {
      // Submit to the queue
      context.setFlightStatus(FlightStatus.READY);
      flightDao.create(context);
      queueFlight(context);
    } else {
      // Submit directly - not allowed if we are shutting down
      if (isQuietingDown()) {
        throw new StairwayShutdownException(
            "Stairway is shutting down and cannot accept a new flight");
      }

      // Give the flight context the public stairway object
      flightDao.create(context);
      launchFlight(context);
    }
  }

  /** Simple submit */
  public void submit(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws StairwayException, InterruptedException, DuplicateFlightIdException {
    submit(flightId, flightClass, inputParameters, false, null);
  }

  /** Submit to the queue */
  public void submitToQueue(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws StairwayException, InterruptedException {
    submit(flightId, flightClass, inputParameters, true, null);
  }

  /** Submit with debug info */
  public void submitWithDebugInfo(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException, DuplicateFlightIdException {
    submit(flightId, flightClass, inputParameters, shouldQueue, debugInfo);
  }

  /**
   * This code trinket is used by submit and WorkQueueListener to decide if there is room It is
   * public so WorkQueueListener can use it.
   *
   * @return true if there is room to take on more work.
   */
  public boolean spaceAvailable() {
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
   * @throws StairwayException other Stairway exceptions
   * @throws StairwayShutdownException Stairway is shutting down and cannot resume a flight
   * @throws DatabaseOperationException failure during flight database operations
   * @throws InterruptedException on shutdown during resume
   */
  public boolean resume(String flightId)
      throws StairwayException, StairwayShutdownException, DatabaseOperationException,
          InterruptedException {
    if (isQuietingDown()) {
      throw new StairwayShutdownException("Stairway is shutting down and cannot resume a flight");
    }

    // The DAO fills in the persistent fields in the flightContext
    FlightContextImpl flightContext = flightDao.resume(stairwayName, flightId);
    if (flightContext == null) {
      return false;
    }

    Flight flight =
        FlightFactory.makeFlightFromName(
            flightContext.getFlightClassName(),
            flightContext.getInputParameters(),
            applicationContext);

    // With the flight, we can complete building the flight context
    flightContext.setDynamicContext(this, flight);
    launchFlight(flightContext);

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
      throws StairwayException, InterruptedException {

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
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException failure to get flight state
   * @throws FlightNotFoundException flight id does not exist
   * @throws FlightWaitTimedOutException if interrupted or polling interval expired
   * @throws InterruptedException on shutdown while waiting for flight completion
   */
  public FlightState waitForFlight(String flightId, Integer pollSeconds, Integer pollCycles)
      throws StairwayException, DatabaseOperationException, FlightNotFoundException,
          FlightWaitTimedOutException, InterruptedException {
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
    throw new FlightWaitTimedOutException("Flight did not complete in the allowed wait time");
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
      throws StairwayException, FlightNotFoundException, DatabaseOperationException,
          InterruptedException {
    return flightDao.getFlightState(flightId);
  }

  /**
   * Implementation of getFlights - not for direct use. See {@link Stairway#getFlights(int, int,
   * FlightFilter)}
   */
  public List<FlightState> getFlights(int offset, int limit, FlightFilter filter)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return flightDao.getFlights(offset, limit, filter);
  }

  /**
   * Implementation of getFlights - not for direct use. See {@link Stairway#getFlights(String,
   * Integer, FlightFilter)}
   */
  public FlightEnumeration getFlights(
      @Nullable String nextPageToken, @Nullable Integer limit, @Nullable FlightFilter filter)
      throws StairwayException, DatabaseOperationException, InterruptedException {
    return flightDao.getFlights(nextPageToken, limit, filter);
  }

  /**
   * @return name of this stairway instance
   */
  public String getStairwayName() {
    return stairwayName;
  }

  // Public so it can be checked from WorkQueueListener
  public boolean isQuietingDown() {
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

  void exitFlight(FlightContextImpl context)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException {
    // save the flight state in the database
    flightDao.exit(context);

    if (context.getFlightStatus() == FlightStatus.READY && queueManager.isWorkQueueEnabled()) {
      queueFlight(context);
    }
    if (context.getFlightStatus() == FlightStatus.READY_TO_RESTART) {
      if (queueManager.isWorkQueueEnabled()) {
        queueFlight(context);
      } else {
        resume(context.getFlightId());
      }
    }
  }

  private void queueFlight(FlightContextImpl flightContext)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException {
    // If the flight state is READY, then we put the flight on the queue and mark it queued in the
    // database. We cannot go directly from RUNNING to QUEUED. Suppose we put the flight in
    // the queue before it was marked QUEUED. Another Stairway instance would see it was
    // RUNNING and decide it shouldn't run it. Suppose we put the flight in the queue after
    // we marked it QUEUED. Then if we fail before we put it on the queue, no one knows it should
    // be queued. To close that window, we use the READY state to decouple the flight from RUNNING.
    // Then we queue, then we mark as queued. Another Stairway looking for orphans, will find the
    // READY and queue it. Putting a flight on the queue twice is not a problem. Stairway
    // instances race to see who gets to run it.
    queueManager.queueReadyFlight(flightContext.getFlightId());
    flightDao.queued(flightContext);
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
  void recoverReady() throws StairwayException, InterruptedException {
    List<String> readyFlightList = flightDao.getReadyFlights();
    for (String flightId : readyFlightList) {
      if (queueManager.isWorkQueueEnabled()) {
        FlightContextImpl flightContext = flightDao.makeFlightContextById(flightId);
        queueFlight(flightContext);
      } else {
        resume(flightId);
      }
    }
  }

  /*
   * Build the FlightRunner object that will execute the flight
   * and hand it to the threadPool to run.
   */
  private void launchFlight(FlightContextImpl flightContext) {
    FlightRunner runner = new FlightRunner(flightContext);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Stairway thread pool: "
              + threadPool.getActiveCount()
              + " active from pool of "
              + threadPool.getPoolSize());
    }
    logger.info("Launching flight " + flightContext.flightDesc());
    threadPool.submit(runner);
  }

  HookWrapper getHookWrapper() {
    return hookWrapper;
  }
}
