package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class QueueMessageTest {

  @Test
  public void messageTest() throws Exception {
    QueueMessageReady messageReady = new QueueMessageReady("abcde");
    String serialized = QueueMessage.serialize(messageReady);
    QueueMessage deserialized = QueueMessage.deserialize(serialized);
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
