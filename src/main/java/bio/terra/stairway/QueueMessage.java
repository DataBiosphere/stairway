package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayExecutionException;
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

abstract class QueueMessage {
  private static final Logger logger = LoggerFactory.getLogger(QueueMessage.class);

  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .registerModule(new ParameterNamesModule())
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .registerModule(new JsonNullableModule())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  protected static final String FORMAT_VERSION = "1";

  abstract void process(Stairway stairway) throws InterruptedException;

  static String serialize(QueueMessage message) throws StairwayExecutionException {
    try {
      return objectMapper.writeValueAsString(message);
    } catch (JsonProcessingException ex) {
      throw new StairwayExecutionException("Failed to serialize message", ex);
    }
  }

  static Boolean processMessage(String message, Stairway stairway) throws InterruptedException {
    QueueMessage qm = deserialize(message);
    if (qm == null) {
      return false;
    }
    qm.process(stairway);
    return true;
  }

  static QueueMessage deserialize(String message) {
    QueueMessageNoFields qmNoFields;
    try {
      qmNoFields = objectMapper.readValue(message, QueueMessageNoFields.class);
    } catch (JsonProcessingException ex) {
      logger.error("Ignoring message: failed to deserialize: " + message, ex);
      return null;
    }

    if (!StringUtils.equals(qmNoFields.getType().getVersion(), FORMAT_VERSION)) {
      logger.error("Ignoring message: unknown version: " + message);
      return null;
    }

    QueueMessageEnum queueMessageEnum = qmNoFields.getType().getMessageEnum();
    if (queueMessageEnum == null) {
      logger.error("Ignoring message: unknown message type: " + message);
      return null;
    }

    QueueMessage qm;
    try {
      qm = objectMapper.readValue(message, queueMessageEnum.getMessageClass());
      return qm;
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
