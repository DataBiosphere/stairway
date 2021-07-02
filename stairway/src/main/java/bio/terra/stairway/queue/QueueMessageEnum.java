package bio.terra.stairway.queue;

import com.fasterxml.jackson.annotation.JsonIgnore;

enum QueueMessageEnum {
  QUEUE_MESSAGE_READY(QueueMessageReady.class); // flight is ready to run

  @JsonIgnore private final Class<? extends QueueMessage> messageClass;

  QueueMessageEnum(Class<? extends QueueMessage> messageClass) {
    this.messageClass = messageClass;
  }

  Class<? extends QueueMessage> getMessageClass() {
    return messageClass;
  }
}
