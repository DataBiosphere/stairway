package bio.terra.stairway;

import com.fasterxml.jackson.annotation.JsonIgnore;

public enum QueueMessageEnum {
    QUEUE_MESSAGE_READY(QueueMessageReady.class);    // flight is ready to run

    @JsonIgnore
    private final Class<? extends QueueMessage> messageClass;

    QueueMessageEnum(Class<? extends QueueMessage> messageClass) {
        this.messageClass = messageClass;
    }

    public Class<? extends QueueMessage> getMessageClass() {
        return messageClass;
    }
}
