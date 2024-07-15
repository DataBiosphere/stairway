package bio.terra.stairway.queue;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.impl.MdcUtils;
import bio.terra.stairway.impl.StairwayImpl;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The Ready message communicates that a flight, identified by flightId, is ready for execution. By
 * the time the message is processed, the flight may no longer be in the ready state, either because
 * duplicate messages are allowed or because some other Stairway found the ready flight through
 * another method. The flight state in the database is the authority on the state of the flight.
 */
class QueueMessageReady extends QueueMessage {
  private static final Logger logger = LoggerFactory.getLogger(QueueMessageReady.class);

  private QueueMessageType type;
  private String flightId;
  private Map<String, String> callingThreadContext;

  private QueueMessageReady() {}

  public QueueMessageReady(String flightId) {
    this.type =
        new QueueMessageType(QueueMessage.FORMAT_VERSION, QueueMessageEnum.QUEUE_MESSAGE_READY);
    this.flightId = flightId;
    this.callingThreadContext = MDC.getCopyOfContextMap();
  }

  @Override
  public boolean process(StairwayImpl stairwayImpl) throws InterruptedException {
    return MdcUtils.callWithContext(
        callingThreadContext,
        () -> {
          try {
            // Resumed is false if the flight is not found in the Ready state. We still call that
            // a complete processing of the message and return true. We assume that some this is a
            // duplicate message or that some other Stairway found the ready flight on recovery.
            boolean resumed = stairwayImpl.resume(flightId);
            logger.info(
                "Stairway "
                    + stairwayImpl.getStairwayName()
                    + (resumed ? " resumed flight: " : " did not find flight to resume: ")
                    + flightId);
            return true;
          } catch (DatabaseOperationException ex) {
            logger.error(
                "Unexpected stairway error, leaving %s on the queue".formatted(flightId), ex);
            return false;
          }
        });
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

  public Map<String, String> getCallingThreadContext() {
    return callingThreadContext;
  }
}
