package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueMessageReady extends QueueMessage {
  private static final Logger logger = LoggerFactory.getLogger(QueueMessageReady.class);

  private QueueMessageType type;
  private String flightId;

  private QueueMessageReady() {}

  QueueMessageReady(String flightId) {
    this.type =
        new QueueMessageType(QueueMessage.FORMAT_VERSION, QueueMessageEnum.QUEUE_MESSAGE_READY);
    this.flightId = flightId;
  }

  @Override
  void process(Stairway stairway) throws InterruptedException {
    try {
      boolean resumed = stairway.resume(flightId);
      logger.info(
          "Stairway "
              + stairway.getStairwayName()
              + (resumed ? " resumed flight: " : " did not resume flight: ")
              + flightId);
    } catch (DatabaseOperationException ex) {
      logger.error("Unexpected stairway error", ex);
    }
  }

  QueueMessageType getType() {
    return type;
  }

  private void setType(QueueMessageType type) {
    this.type = type;
  }

  String getFlightId() {
    return flightId;
  }

  void setFlightId(String flightId) {
    this.flightId = flightId;
  }
}
