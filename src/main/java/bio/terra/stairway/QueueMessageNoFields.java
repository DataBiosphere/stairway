package bio.terra.stairway;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// We use this class to read the type information, to guide deserializing the right type
@JsonIgnoreProperties(ignoreUnknown = true)
class QueueMessageNoFields extends QueueMessage {
  private QueueMessageType type;

  public QueueMessageType getType() {
    return type;
  }

  public void setType(QueueMessageType type) {
    this.type = type;
  }

  @Override
  public void process(Stairway stairway) {}
}
