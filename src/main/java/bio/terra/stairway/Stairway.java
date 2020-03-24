package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stairway is the object that drives execution of Flights.
 */
public class Stairway {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    private ExecutorService threadPool;
    private FlightDao flightDao;
    private Object applicationContext;
    private ExceptionSerializer exceptionSerializer;
    private String stairwayName;
    private String stairwayId;
    private AtomicBoolean quietingDown; // flights test this and force yield if true

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
     * Note: the default exceptionSerializer is used in this form of the constructor and the
     * stairwayId is generated.
     */
    public Stairway(ExecutorService threadPool, Object applicationContext) {
        this(threadPool, applicationContext, new DefaultExceptionSerializer(), ShortUUID.get());
    }

    /**
     * Alternate form of the Stairway constructor that allows the client to provide their own exception
     * serializer class.
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     * @param applicationContext an object passed through to flights; otherwise unused by Stairway
     * @param exceptionSerializer implementation of ExceptionSerializer interface used for exception serde.
     *                            If null, the Stairway default exception serializer is used.
     * @param stairwayName a unique name for this Stairway instance. In a Kubernetes environment, this
     *                   might be derived from the pod name.
     */
    public Stairway(ExecutorService threadPool,
                    Object applicationContext,
                    ExceptionSerializer exceptionSerializer,
                    String stairwayName) {
        this.threadPool = threadPool;
        this.applicationContext = applicationContext;
        this.exceptionSerializer =
                (exceptionSerializer == null) ? new DefaultExceptionSerializer() : exceptionSerializer;
        this.stairwayName = stairwayName;
        this.quietingDown = new AtomicBoolean();
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
            // If cleaning, set the stairway id after cleaning (or it gets cleaned!)
            flightDao.startClean();
            stairwayId = flightDao.findOrCreateStairwayInstance(stairwayName);
        } else {
            stairwayId = flightDao.findOrCreateStairwayInstance(stairwayName);
            recoverFlights();
        }
    }

    /**
     * Graceful shutdown: instruct stairway to stop executing flights.
     * When running flights hit a step boundary they will yield. No new flights are
     * able to start. Then this thread waits for termination of the thread pool;
     * basically, just exposing the awaitTermination parameters.
     * @param waitTimeout time, in some units to wait before timing out
     * @param unit the time unit of waitTimeout.
     * @return true if we quieted; false if we were interrupted or timed out before quieting down
     */
    public boolean quietDown(long waitTimeout, TimeUnit unit) {
        quietingDown.set(true);
        threadPool.shutdown();
        try {
            return threadPool.awaitTermination(waitTimeout, unit);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Not-so-graceful shutdown: shutdown the pool which will cause an InterruptedException on all of
     * the flights. That _should_ cause the flights to rapidly terminate and get set to unowned.
     */
    public void terminate() {
        threadPool.shutdownNow();
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
        if (isQuietingDown()) {
            throw new MakeFlightException("Stairway is shutting down and cannot accept a new flight");
        }

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
     * Try to resume a flight. If the flight is unowned and either in WAITING or READY state, then
     * this Stairway takes ownership and executes the rest of the flight. There can be race conditions
     * with other Stairway's on resume. It is not an error if the flight is not resumed by this
     * Stairway.
     *
     * @param flightId the flight to try to resume
     * @return true if this Stairway owns and is executing the flight; false if ownership could not be claimed
     * @throws DatabaseOperationException failure during flight database operations
     */
    public boolean resume(String flightId) throws DatabaseOperationException {
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
     * Delete a flight - this removes the database record of a flight.
     * Setting force delete to true allows deleting a flight that is not marked as done. We allow that
     * in case the flight state gets corrupted somehow and needs manual cleanup.
     * If force delete is false, and the flight is not done, we throw.
     *
     * @param flightId flight to delete
     * @param forceDelete boolean to allow force deleting of flight database state, regardless of flight state
     * @throws DatabaseOperationException errors from removing the flight in memory and in database
     */
    public void deleteFlight(String flightId, boolean forceDelete) throws DatabaseOperationException {
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
     */
    @Deprecated
    public FlightState waitForFlight(String flightId, Integer pollSeconds, Integer pollCycles)
            throws DatabaseOperationException, FlightException {
        int sleepSeconds = (pollSeconds == null) ? 10 : pollSeconds;
        int pollCount = 0;

        try {
            while (pollCycles == null || pollCount < pollCycles) {
                // loop getting flight state and sleeping
                TimeUnit.SECONDS.sleep(sleepSeconds);
                FlightState state = getFlightState(flightId);
                if (!state.isActive()) {
                    return state;
                }
                pollCount++;
            }
        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            throw new FlightException("Stairway was interrupted");
        }
        throw new FlightException("Flight did not complete in the allowed wait time");
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
        return flightDao.getFlightState(flightId);
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

    String getStairwayId() {
        return stairwayId;
    }

    boolean isQuietingDown() {
        return quietingDown.get();
    }

    // Exposed for unit testing
    FlightDao getFlightDao() {
        return flightDao;
    }

    /**
     * Find any incomplete flights and recover them. We overwrite the flight context of this flight
     * with the recovered flight context. The normal constructor path needs to give the input parameters
     * to the flight subclass. This is a case where we don't really want to have the Flight object set up
     * its own context. It is simpler to override it than to make a separate code path for this recovery case.
     *
     * The flightlog records the last operation performed; so we need to set the execution point to the next
     * step index.
     *
     * @throws DatabaseOperationException on database errors
     */
    private void recoverFlights() throws DatabaseOperationException {
        List<FlightContext> flightList = flightDao.recover(stairwayId);
        for (FlightContext flightContext : flightList) {
            resumeOneFlight(flightContext);
        }
    }

    private void resumeOneFlight(FlightContext flightContext) {
        Flight flight = makeFlightFromName(flightContext.getFlightClassName(), flightContext.getInputParameters());
        flightContext.nextStepIndex();
        flightContext.setStairway(this);
        flight.setFlightContext(flightContext);
        launchFlight(flight);
    }

    /*
     * Build the task context to keep track of the running flight.
     * Once it is launched, hook it into the {@link #taskContextMap} so other
     * calls can resolve it.
     */
    private void launchFlight(Flight flight) {
        // Give the flight the flightDao object so it can properly record its steps and be found
        flight.setFlightDao(flightDao);

        if (logger.isDebugEnabled()) {
            if (threadPool instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) threadPool;
                logger.debug("Stairway thread pool: " + tpe.getActiveCount() +
                        " active from pool of " + tpe.getPoolSize());
            }
        }
        logger.info("Launching flight " + flight.context().getFlightClassName());
        threadPool.submit(flight);
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
                .toHashCode();
    }
}
