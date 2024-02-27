package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.FlightWaitTimedOutException;
import bio.terra.stairway.exception.MigrateException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.exception.StairwayShutdownException;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * The Stairway object represents a Stairway instance within which Flights can be run. This
 * interface provides the methods clients should use to operate the Stairway instance.
 *
 * <p>Stairway initialization is done in three steps.
 *
 * <p>The first step is the construction of the Staiway object. The StairwayBuilder build() method
 * constructs the implementation object providing the Stairway interface. It does little beyond
 * remembering the inputs. It does not do any database activity. That lets the rest of the
 * application come up and do any database configuration.
 *
 * <p>The second step is the 'initialize' call (below) that performs any necessary database
 * initialization and migration. It sets up the flightDao and returns the current list of Stairway
 * instances recorded in the database.
 *
 * <p>The third step is the 'recover-and-start' call (further below) that performs any requested
 * recovery and opens this Stairway for business. Some flights may already be running depending on
 * how recovery went. They get first crack at the resources. Then submissions from the API and the
 * Work Queue are enabled.
 */
public interface Stairway {

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
  List<String> initialize(DataSource dataSource, boolean forceCleanStart, boolean migrateUpgrade)
      throws StairwayShutdownException, DatabaseOperationException, MigrateException,
          StairwayException, InterruptedException;

  /**
   * Third step of initialization
   *
   * <p>recoverAndStart will do recovery on any obsolete Stairway instances passed in by the caller.
   * Presumably, an edited list of what was returned by the initialize call above.
   *
   * <p>It makes a scan for ready flights and queues them (or launches if no queue).
   *
   * @param obsoleteStairways list of stairways to recover
   * @throws StairwayException other Stairway berror
   * @throws DatabaseOperationException database access failure
   * @throws StairwayExecutionException stairway error
   * @throws InterruptedException interruption during recovery/startup
   */
  void recoverAndStart(List<String> obsoleteStairways)
      throws StairwayException, DatabaseOperationException, InterruptedException,
          StairwayExecutionException;

  /**
   * Recover any orphaned flights from a particular Stairway instance. This method can be called
   * when a server using Stairway discovers that another Stairway instance has failed. For example,
   * when a Kubernetes listener notices a pod failure.
   *
   * @param stairwayName name of a stairway instance to recover
   * @throws InterruptedException interruption during recovery
   */
  void recoverStairway(String stairwayName) throws InterruptedException;

  /**
   * Graceful shutdown: instruct stairway to stop executing flights. When running flights hit a step
   * boundary they will yield. No new flights are able to start. Then this thread waits for
   * termination of the thread pool; basically, just exposing the awaitTermination parameters.
   *
   * @param waitTimeout time, in some units to wait before timing out
   * @param unit the time unit of waitTimeout.
   * @return true if we quieted; false if we were interrupted or timed out before quieting down
   */
  boolean quietDown(long waitTimeout, TimeUnit unit);

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
  boolean terminate(long waitTimeout, TimeUnit unit) throws StairwayException, InterruptedException;

  /**
   * Method to generate a flight id. This is a convenience method to allow clients to generate
   * compliant flight ids for Stairway. You don't have to use it.
   *
   * @return 22 character, base64url-encoded UUID
   * @see <a href="https://base64.guru/standards/base64url">Base64 URL</a>
   */
  String createFlightId();

  /**
   * Submit a flight for execution.
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException failure during flight object creation, persisting to
   *     database or launching
   * @throws StairwayExecutionException failure queuing the flight * @throws
   * @throws DuplicateFlightIdException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  void submit(String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException, DuplicateFlightIdException;

  /**
   * Submit a flight to queue for execution.
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException failure during flight object creation, persisting to
   *     database or launching
   * @throws StairwayExecutionException failure queuing the flight
   * @throws DuplicateFlightIdException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  void submitToQueue(
      String flightId, Class<? extends Flight> flightClass, FlightMap inputParameters)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException, DuplicateFlightIdException;

  /**
   * Submit a flight with debug information. This is intended for testing, and not for production.
   *
   * @param flightId Stairway allows clients to choose flight ids. That lets a client record an id,
   *     perhaps persistently, before the flight is run. Stairway requires that the ids be unique in
   *     the scope of a Stairway instance. As a convenience, you can use {@link #createFlightId()}
   *     to generate globally unique ids.
   * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
   * @param inputParameters key-value map of parameters to the flight
   * @param shouldQueue true if the flight should be put in the queue; false if we should try to run
   *     it in this Stairway instance
   * @param debugInfo debug info object
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException failure during flight object creation, persisting to
   *     database or launching
   * @throws StairwayExecutionException failure queuing the flight
   * @throws DuplicateFlightIdException provided flight id already exists
   * @throws InterruptedException this thread was interrupted
   */
  void submitWithDebugInfo(
      String flightId,
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      boolean shouldQueue,
      FlightDebugInfo debugInfo)
      throws StairwayException, DatabaseOperationException, StairwayExecutionException,
          InterruptedException, DuplicateFlightIdException;

  /**
   * Wait for a flight to complete
   *
   * <p>This is a very simple polling method to help you get started with Stairway. It may not be
   * what you want for production code.
   *
   * @param flightId the flight to wait for
   * @param pollSeconds sleep time for each poll cycle; if null, defaults to 10 seconds
   * @param pollCycles number of times to poll; if null, we poll forever
   * @return flight state object
   * @throws StairwayException other Stairway errors
   * @throws DatabaseOperationException failure to get flight state * @throws
   * @throws FlightNotFoundException flight id does not exist
   * @throws FlightWaitTimedOutException if interrupted or polling interval expired
   * @throws InterruptedException on shutdown while waiting for flight completion
   */
  FlightState waitForFlight(String flightId, Integer pollSeconds, Integer pollCycles)
      throws StairwayException, DatabaseOperationException, FlightNotFoundException,
          FlightWaitTimedOutException, InterruptedException;

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
  boolean resume(String flightId)
      throws StairwayException, StairwayShutdownException, DatabaseOperationException,
          InterruptedException;

  /**
   * The Stairway Control interface provides access to Staiwray internals for debugging and flight
   * recovery.
   *
   * @return Control interface
   */
  Control getControl();

  /**
   * Get the state of a specific flight If the flight is complete and still in our in-memory map, we
   * remove it. The logic is that if getFlightState is called, then either the wait finished or we
   * are polling and won't perform a wait.
   *
   * @param flightId identifies the flight to retrieve state for
   * @return FlightState state of the flight
   * @throws StairwayException other Stairway exception
   * @throws FlightNotFoundException flignt was not found
   * @throws DatabaseOperationException not found in the database or unexpected database issues
   * @throws InterruptedException on shutdown
   */
  public FlightState getFlightState(String flightId)
      throws StairwayException, FlightNotFoundException, DatabaseOperationException,
          InterruptedException;

  /**
   * NOTE: This endpoint is maintained for backward compatibility. We recommend switching to the new
   * getFlights endpoint below.
   *
   * <p>Enumerate flights - returns a range of flights ordered by submit time. Note that there can
   * be "jitter" in the paging through flights if new flights are submitted.
   *
   * <p>You can add one or more predicates in a filter list. The filters are logically ANDed
   * together and applied to the input parameters of flights. That lets you add input parameters
   * (like who is running the flight) and then select by that to show flights being run by that
   * user. {@link FlightFilter} documents the different filters and their arguments.
   *
   * <p>The offset and limit are applied after the filtering is done.
   *
   * @param offset offset of the row ordered by most recent flight first
   * @param limit limit the number of rows returned
   * @param filter predicates to apply to filter flights and retrieval options
   * @return List of FlightState
   * @throws StairwayException - other Stairway error
   * @throws DatabaseOperationException unexpected database errors
   * @throws InterruptedException on shutdown
   */
  List<FlightState> getFlights(int offset, int limit, FlightFilter filter)
      throws StairwayException, DatabaseOperationException, InterruptedException;

  /**
   * Enumerate flights - returns a range of flights ordered by submit time. Use of the page token
   * eliminates jitter issues in the original getFlights endpoint.
   *
   * @param nextPageToken starting point for the next page of data. Null means start at the
   *     beginning of the result set.
   * @param limit limit the number of rows returned. Null means no limit: return all rows
   * @param filter predicates to apply to filter flights
   * @return FlightEnumeration including the total flights in the filtered set, the encoded token
   *     for the next page of results, as well as the list of Flightstate objects.
   * @throws StairwayException - other Stairway error
   * @throws DatabaseOperationException unexpected database errors
   * @throws InterruptedException on shutdown
   */
  FlightEnumeration getFlights(
      @Nullable String nextPageToken, @Nullable Integer limit, @Nullable FlightFilter filter)
      throws StairwayException, DatabaseOperationException, InterruptedException;

  /**
   * @return name of this stairway instance
   */
  String getStairwayName();
}
