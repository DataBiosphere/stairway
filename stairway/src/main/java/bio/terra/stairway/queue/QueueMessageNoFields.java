package bio.terra.stairway.queue;

import bio.terra.stairway.impl.StairwayImpl;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A message of this format is never put on the work queue. We only use this class to read the
 * message type information. With the type information, we can then properly deserialize the right
 * QueueMessage subclass.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class QueueMessageNoFields extends QueueMessage {
  private QueueMessageType type;

  public QueueMessageType getType() {
    return type;
  }

  public void setType(QueueMessageType type) {
    this.type = type;
  }

  // Never used. Just here to comply with the abstract base class.
  @Override
  public boolean process(StairwayImpl stairwayImpl) {
    return true;
  }
}
