package bio.terra.stairway.queue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class QueueMessageTest {

  @Test
  public void messageTest() throws Exception {
    WorkQueueProcessor queueProcessor = new WorkQueueProcessor(null);
    QueueMessageReady messageReady = new QueueMessageReady("abcde");
    String serialized = queueProcessor.serialize(messageReady);
    QueueMessage deserialized = queueProcessor.deserialize(serialized);
    if (deserialized instanceof QueueMessageReady) {
      QueueMessageReady messageReadyCopy = (QueueMessageReady) deserialized;
      assertThat(messageReadyCopy.getFlightId(), equalTo(messageReady.getFlightId()));
      assertThat(
          messageReadyCopy.getType().getMessageEnum(),
          equalTo(messageReady.getType().getMessageEnum()));
      assertThat(
          messageReadyCopy.getType().getVersion(), equalTo(messageReady.getType().getVersion()));
      assertThat(messageReadyCopy.getFlightId(), equalTo(messageReady.getFlightId()));
    } else {
      fail();
    }
  }
}
