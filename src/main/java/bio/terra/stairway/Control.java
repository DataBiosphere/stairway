package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import java.util.List;

/**
 * Control class can be retrieved from the Stairway object and provides methods helpful
 * for debugging and recovery.
 */
public class Control {
  private final FlightDao flightDao;

  public Control(FlightDao flightDao) {
    this.flightDao = flightDao;
  }

  /**
   * Lookup a flight without the completed time constraint
   * @param flightId identifier of the flight to look up
   * @return FlightState
   * @throws FlightNotFound - no flight
   * @throws DatabaseOperationException - unexpected error in the operation
   * @throws InterruptedException - interrupt
   */
  public FlightState getFlightState(String flightId)
      throws DatabaseOperationException, InterruptedException {
    return flightDao.controlGetFlightState(flightId);
  }

  /**
   * Enumerate flights without the completed time constraint
   *
   * @param offset offset into the result set to start returning
   * @param limit max number of results to return
   * @param inFilter filters to apply to the flights
   * @return list of FlightState objects for the filtered, paged flights
   * @throws DatabaseOperationException on all database issues
   * @throws InterruptedException thread shutdown
   */
  public List<FlightState> getFlights(int offset, int limit, FlightFilter inFilter)
      throws DatabaseOperationException, InterruptedException {
    return flightDao.controlGetFlights(offset, limit, inFilter);
  }
}
