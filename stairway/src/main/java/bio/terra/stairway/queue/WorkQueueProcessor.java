package bio.terra.stairway.queue;

import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.impl.StairwayImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.commons.lang3.StringUtils;
import org.openapitools.jackson.nullable.JsonNullableModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Module for serializing, deserializing, and processing messages from the work queue. */
public class WorkQueueProcessor {
  private static final Logger logger = LoggerFactory.getLogger(WorkQueueProcessor.class);

  private final StairwayImpl stairwayImpl;

  public WorkQueueProcessor(StairwayImpl stairwayImpl) {
    this.stairwayImpl = stairwayImpl;
  }

  /**
   * Implementation of QueueProcessFunction, this is called from the dispatchMessage method of the
   * various queue implementations. It deserialized the message from the queue into a QueueMessage
   * object and then asks that object to process the message.
   *
   * @param message message to be processed
   * @return true if we processed successfully; false otherwise
   * @throws InterruptedException on shutdown interrupt
   */
  public Boolean processMessage(String message) throws InterruptedException {
    QueueMessage qm = deserialize(message);
    if (qm == null) {
      return false;
    }
    return qm.process(stairwayImpl);
  }

  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .registerModule(new ParameterNamesModule())
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .registerModule(new JsonNullableModule())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  String serialize(QueueMessage message) throws StairwayExecutionException {
    try {
      return objectMapper.writeValueAsString(message);
    } catch (JsonProcessingException ex) {
      throw new StairwayExecutionException("Failed to serialize message: " + message, ex);
    }
  }

  QueueMessage deserialize(String message) {
    QueueMessageNoFields qmNoFields;
    try {
      qmNoFields = objectMapper.readValue(message, QueueMessageNoFields.class);
    } catch (JsonProcessingException ex) {
      logger.error("Ignoring message: failed to deserialize: " + message, ex);
      return null;
    }

    if (!StringUtils.equals(qmNoFields.getType().getVersion(), QueueMessage.FORMAT_VERSION)) {
      logger.error("Ignoring message: unknown version: " + message);
      return null;
    }

    QueueMessageEnum queueMessageEnum = qmNoFields.getType().getMessageEnum();
    if (queueMessageEnum == null) {
      logger.error("Ignoring message: unknown message type: " + message);
      return null;
    }

    try {
      return objectMapper.readValue(message, queueMessageEnum.getMessageClass());
    } catch (JsonProcessingException ex) {
      logger.error(
          "Ignoring message: failed to deserialize type: "
              + queueMessageEnum.name()
              + " message: "
              + message);
      return null;
    }
  }
}
