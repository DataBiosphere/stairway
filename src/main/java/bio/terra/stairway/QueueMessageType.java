package bio.terra.stairway;

// An instance of this class, serialized with the name "type" must be included in all QueueMessages.
// That lets all message classes be easily serialized/deserialized POJOs without inheritance.
// This class must remain constant across other changes to the message format.
public class QueueMessageType {
  private String version;
  private QueueMessageEnum messageEnum;

  private QueueMessageType() {}

  public QueueMessageType(String version, QueueMessageEnum messageEnum) {
    this.version = version;
    this.messageEnum = messageEnum;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public QueueMessageEnum getMessageEnum() {
    return messageEnum;
  }

  public void setMessageEnum(QueueMessageEnum messageEnum) {
    this.messageEnum = messageEnum;
  }
}
