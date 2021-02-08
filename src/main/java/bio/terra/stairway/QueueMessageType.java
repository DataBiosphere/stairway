package bio.terra.stairway;

/**
 * An instance of this class, serialized with the name "type" must be included in all QueueMessages.
 * That lets all message classes be easily serialized/deserialized POJOs without inheritance. This
 * class must remain constant across other changes to the message format.
 */
class QueueMessageType {
  private String version;
  private QueueMessageEnum messageEnum;

  private QueueMessageType() {}

  QueueMessageType(String version, QueueMessageEnum messageEnum) {
    this.version = version;
    this.messageEnum = messageEnum;
  }

  String getVersion() {
    return version;
  }

  void setVersion(String version) {
    this.version = version;
  }

  QueueMessageEnum getMessageEnum() {
    return messageEnum;
  }

  void setMessageEnum(QueueMessageEnum messageEnum) {
    this.messageEnum = messageEnum;
  }
}
