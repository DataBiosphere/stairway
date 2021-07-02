package bio.terra.stairway.queue;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.impl.StairwayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueMessageReady extends QueueMessage {
  private static final Logger logger = LoggerFactory.getLogger(QueueMessageReady.class);

  private QueueMessageType type;
  private String flightId;

  private QueueMessageReady() {}

  public QueueMessageReady(String flightId) {
    this.type =
        new QueueMessageType(QueueMessage.FORMAT_VERSION, QueueMessageEnum.QUEUE_MESSAGE_READY);
    this.flightId = flightId;
  }

  @Override
  public void process(Object dispatchContext) throws InterruptedException {
    StairwayImpl stairwayImpl = (StairwayImpl) dispatchContext;
    try {
      boolean resumed = stairwayImpl.resume(flightId);
      logger.info(
          "Stairway "
              + stairwayImpl.getStairwayName()
              + (resumed ? " resumed flight: " : " did not resume flight: ")
              + flightId);
    } catch (DatabaseOperationException ex) {
      logger.error("Unexpected stairway error", ex);
    }
  }

  public QueueMessageType getType() {
    return type;
  }

  public void setType(QueueMessageType type) {
    this.type = type;
  }

  public String getFlightId() {
    return flightId;
  }

  public void setFlightId(String flightId) {
    this.flightId = flightId;
  }
}
