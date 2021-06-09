package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This cleaner threat initiates deletion of completed flights that are older than the retention
 * time set for this stairway.
 */
class CompletedFlightCleaner implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CompletedFlightCleaner.class);
  private final Duration retention;
  private final FlightDao flightDao;

  CompletedFlightCleaner(Duration retention, FlightDao flightDao) {
    this.retention = retention;
    this.flightDao = flightDao;
  }

  @Override
  public void run() {
    Instant deleteOlderThan = Instant.now().minus(retention);
    logger.info("Removing flights completed before {}", deleteOlderThan);
    try {
      int count = flightDao.deleteCompletedFlights(deleteOlderThan);
      logger.info("Cleaned up {} completed flights", count);
    } catch (DatabaseOperationException | InterruptedException ex) {
      logger.warn("Error removing flights", ex);
    }
  }
}
