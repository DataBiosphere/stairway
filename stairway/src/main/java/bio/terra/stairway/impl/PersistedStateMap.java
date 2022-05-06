package bio.terra.stairway.impl;

import bio.terra.stairway.FlightMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The persisted state class holds a FlightMap that is written back to the database when it is
 * explicitly flushed. The persisted state is connected to a flight, but is written independently of
 * step logging.
 *
 * <p>Right now, it is only used within ProgressMetersImpl to support flights recording their
 * progress. However, if other use cases arise, we may expose it directly to flights.
 */
public class PersistedStateMap extends FlightMap {
  private static final Logger logger = LoggerFactory.getLogger(PersistedStateMap.class);

  private final FlightDao flightDao;
  private final String flightId;

  /**
   * @param flightDao DAO for persisting the map
   * @param flightId flightId associated with the state
   */
  public PersistedStateMap(FlightDao flightDao, String flightId) {
    this.flightDao = flightDao;
    this.flightId = flightId;
  }

  /**
   * Write the flight map to the database
   *
   * @throws InterruptedException on interrupted database wait
   */
  public void flush() throws InterruptedException {
    flightDao.storePersistedStateMap(flightId, this);
  }
}
