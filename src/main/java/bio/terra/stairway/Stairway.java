package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import bio.terra.stairway.exception.MigrateException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stairway is the object that drives execution of Flights. The class is constructed
 * with inputs that allow the caller to specify the thread pool, the flightDao source, and the
 * table name stem to use.
 *
 * Each Stairway runs, logs, and recovers independently.
 *
 * There are two techniques you can use to wait for a flight. One is polling by calling getFlightState. That
 * reads the flight state from the stairway flightDao so will report correct state for as long as the flight
 * lives in the flightDao. (Since we haven't implemented pruning, that means forever.) If you poll in this way,
 * then the in-memory resources are released on the first call to getFlightState that reports the flight has
 * completed in some way.
 *
 * The other technique is to poll the flight future using the isDone() method or block waiting for the
 * flight to complete using the waitForFlight() method. In this case, the in-memory resources are freed
 * when either method detects that the flight is done.
 */
public class Stairway {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    // For each task we start, we make a task context. It lets us look up the results

    static class TaskContext {
        private FutureTask<FlightState> futureResult;
        private Flight flight;

        TaskContext(FutureTask<FlightState> futureResult, Flight flight) {
            this.futureResult = futureResult;
            this.flight = flight;
        }

        FutureTask<FlightState> getFutureResult() {
            return futureResult;
        }

        Flight getFlight() {
            return flight;
        }
    }

    private ConcurrentHashMap<String, TaskContext> taskContextMap;
    private ExecutorService threadPool;
    private FlightDao flightDao;
    private Object applicationContext;
    private ExceptionSerializer exceptionSerializer;

    /**
     * We do initialization in two steps. The constructor does the first step of constructing the object
     * and remembering the inputs. It does not do any flightDao activity. That lets the rest of the
     * application come up and do any database configuration.
     *
     * The second step is the 'initialize' call (below) that sets up the flightDao and performs
     * any recovery needed.
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     * @param applicationContext an object passed through to flights; otherwise unused by Stairway
     * Note: the default exceptionSerializer is used in this form of the constructor.
     */
    public Stairway(ExecutorService threadPool, Object applicationContext) {
        this(threadPool, applicationContext, new DefaultExceptionSerializer());
    }

    /**
     * Alternate form of the Stairway constructor that allows the client to provide their own exception
     * serializer class.
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     * @param applicationContext an object passed through to flights; otherwise unused by Stairway
     * @param exceptionSerializer implementation of ExceptionSerializer interface used for exception serde.
     */
    public Stairway(ExecutorService threadPool, Object applicationContext, ExceptionSerializer exceptionSerializer) {
        this.threadPool = threadPool;
        this.applicationContext = applicationContext;
        this.taskContextMap = new ConcurrentHashMap<>();
        this.exceptionSerializer = exceptionSerializer;
    }

    /**
     * Second step of initialization
     * @param dataSource database to be used to store Stairway data
     * @param forceCleanStart true will drop any existing stairway data. Otherwise existing flights are recovered.
     * @param migrateUpgrade true will run the migrate to upgrade the database
     * @throws DatabaseSetupException failed to clean the database on startup
     * @throws DatabaseOperationException failures to perform recovery
     * @throws MigrateException migration failures
     */
    public void initialize(DataSource dataSource, boolean forceCleanStart, boolean migrateUpgrade)
        throws DatabaseSetupException, DatabaseOperationException, MigrateException {

        if (migrateUpgrade) {
            Migrate migrate = new Migrate();
            migrate.initialize("stairway/db/changelog.xml", dataSource);
        }

        this.flightDao = new FlightDao(dataSource, exceptionSerializer);
        if (forceCleanStart) {
            flightDao.startClean();
        } else {
            recoverFlights();
        }
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
     * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id, perhaps
     *                 persistently, before the flight is run. Stairway requires that the ids be unique in the
     *                 scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
     *                 to generate globally unique ids.
     * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
     * @param inputParameters key-value map of parameters to the flight
     * @throws DatabaseOperationException failure during flight object creation, persisting to database or launching
     */
    public void submit(String flightId,
                       Class<? extends Flight> flightClass,
                       FlightMap inputParameters) throws DatabaseOperationException {

        if (flightClass == null || inputParameters == null) {
            throw new MakeFlightException("Must supply non-null flightClass and inputParameters to submit");
        }
        Flight flight = makeFlight(flightClass, inputParameters);

        flight.context().setFlightId(flightId);
        flight.context().setStairway(this);
        flightDao.submit(flight.context());
        launchFlight(flight);
    }

    /**
     * Delete a flight - this removes the execution context and the database record of a flight.
     * Note that this does allow deleting a flight that is not marked as done. We allow that
     * in case the flight state gets corrupted somehow and needs forceable clean-up.
     *
     * @param flightId flight to delete
     * @throws DatabaseOperationException errors from removing the flight in memory and in database
     */
    public void deleteFlight(String flightId) throws DatabaseOperationException {
        releaseFlight(flightId);
        flightDao.delete(flightId);
    }

    /**
     * Wait for a flight to complete. When it completes, the flight is removed from the taskContextMap.
     *
     * @param flightId the flight to wait for
     * @return flight state object
     * @throws FlightException flight does not exist
     */
    public FlightState waitForFlight(String flightId) throws FlightException {
        TaskContext taskContext = lookupFlight(flightId);

        try {
            FlightState state = taskContext.getFutureResult().get();
            releaseFlight(flightId);
            return state;
        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            throw new FlightException("Stairway was interrupted");
        } catch (ExecutionException ex) {
            throw new FlightException("Unexpected flight exception", ex);
        }
    }

    /**
     * Get the state of a specific flight
     * If the flight is complete and still in our in-memory map, we remove it.
     * The logic is that if getFlightState is called, then either the wait
     * finished or we are polling and won't perform a wait.
     *
     * @param flightId identifies the flight to retrieve state for
     * @return FlightState state of the flight
     * @throws DatabaseOperationException not found in the database or unexpected database issues
     */
    public FlightState getFlightState(String flightId) throws DatabaseOperationException {
        FlightState flightState = flightDao.getFlightState(flightId);
        if (flightState.getFlightStatus() != FlightStatus.RUNNING) {
            releaseFlight(flightId);
        }
        return flightState;
    }

    /**
     * Enumerate flights - returns a range of flights ordered by submit time.
     * Note that there can be "jitter" in the paging through flights if new flights
     * are submitted.
     *
     * You can add one or more predicates in a filter list. The filters are logically
     * ANDed together and applied to the input parameters of flights. That lets you
     * add input parameters (like who is running the flight) and then select by that
     * to show flights being run by that user.
     * {@link FlightFilter} documents the different filters and their arguments.
     *
     * The and limit are applied after the filtering is done.
     *
     * @param offset offset of the row ordered by most recent flight first
     * @param limit limit the number of rows returned
     * @param filter predicates to apply to filter flights
     * @return List of FlightState
     * @throws DatabaseOperationException unexpected database errors
     */
    public List<FlightState> getFlights(int offset, int limit, FlightFilter filter)
            throws DatabaseOperationException {
        return flightDao.getFlights(offset, limit, filter);
    }

    private void releaseFlight(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext != null) {
            if (!taskContext.getFutureResult().isDone()) {
                logger.warn("Removing flight context for in progress flight " + flightId);
            }
            taskContextMap.remove(flightId);
        }
    }

    private TaskContext lookupFlight(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext == null) {
            throw new FlightNotFoundException("Flight '" + flightId + "' not found");
        }
        return taskContext;
    }

    /*
     * Find any incomplete flights and recover them. We overwrite the flight context of this flight
     * with the recovered flight context. The normal constructor path needs to give the input parameters
     * to the flight subclass. This is a case where we don't really want to have the Flight object set up
     * its own context. It is simpler to override it than to make a separate code path for this recovery case.
     *
     * The flightlog records the last operation performed; so we need to set the execution point to the next
     * step index.
     */
    private void recoverFlights() throws DatabaseOperationException {
        List<FlightContext> flightList = flightDao.recover();
        for (FlightContext flightContext : flightList) {
            Flight flight = makeFlightFromName(flightContext.getFlightClassName(), flightContext.getInputParameters());
            flightContext.nextStepIndex();
            flightContext.setStairway(this);
            flight.setFlightContext(flightContext);
            launchFlight(flight);
        }
    }

    /*
     * Build the task context to keep track of the running flight.
     * Once it is launched, hook it into the {@link #taskContextMap} so other
     * calls can resolve it.
     */
    private void launchFlight(Flight flight) {
        // Give the flight the flightDao object so it can properly record its steps
        flight.setFlightDao(flightDao);

        // Build the task context to keep track of the running task
        TaskContext taskContext = new TaskContext(new FutureTask<FlightState>(flight), flight);

        if (logger.isDebugEnabled()) {
            if (threadPool instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) threadPool;
                logger.debug("Stairway thread pool: " + tpe.getActiveCount() +
                        " active from pool of " + tpe.getPoolSize());
            }
        }
        logger.info("Launching flight " + flight.context().getFlightClassName());

        threadPool.execute(taskContext.getFutureResult());

        // Now that it is in the pool, hook it into the map so other calls can resolve it.
        taskContextMap.put(flight.context().getFlightId(), taskContext);
    }

    /*
     * Create a Flight instance given the class name of the derived class of Flight
     * and the input parameters.
     *
     * Note that you can adjust the steps you generate based on the input parameters.
     */
    private Flight makeFlight(
        Class<? extends Flight> flightClass, FlightMap inputParameters) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(FlightMap.class, Object.class);
            Flight flight = (Flight)constructor.newInstance(inputParameters, applicationContext);
            return flight;
        } catch (InvocationTargetException |
                NoSuchMethodException |
                InstantiationException |
                IllegalAccessException ex) {
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
            throw new MakeFlightException("Failed to make a flight from class name '" + className +
                    "' - it is not a subclass of Flight");

        } catch (ClassNotFoundException ex) {
            throw new MakeFlightException("Failed to make a flight from class name '" + className +
                    "'", ex);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("taskContextMap", taskContextMap)
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
                .append(taskContextMap, stairway.taskContextMap)
                .append(threadPool, stairway.threadPool)
                .append(flightDao, stairway.flightDao)
                .append(applicationContext, stairway.applicationContext)
                .append(exceptionSerializer, stairway.exceptionSerializer)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(logger)
                .append(taskContextMap)
                .append(threadPool)
                .append(flightDao)
                .append(applicationContext)
                .append(exceptionSerializer)
                .toHashCode();
    }
}
