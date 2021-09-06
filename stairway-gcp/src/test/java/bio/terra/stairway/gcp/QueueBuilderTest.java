package bio.terra.stairway.gcp;

import bio.terra.stairway.gcp.GcpPubSubQueue.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class QueueBuilderTest {

  @Test
  public void missingArguments() {
    // Test the cases where builder arguments are not all specified
    {
      GcpPubSubQueue.Builder builder = new Builder().topicId("abc").subscriptionId("def");
      Assertions.assertThrows(NullPointerException.class, builder::build);
    }
    {
      GcpPubSubQueue.Builder builder =
          new Builder().projectId("").topicId("abc").subscriptionId("def");
      Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    }

    {
      GcpPubSubQueue.Builder builder = new Builder().projectId("abc").subscriptionId("def");
      Assertions.assertThrows(NullPointerException.class, builder::build);
    }
    {
      GcpPubSubQueue.Builder builder =
          new Builder().projectId("abc").topicId("").subscriptionId("def");
      Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    }

    {
      GcpPubSubQueue.Builder builder = new Builder().projectId("abc").topicId("def");
      Assertions.assertThrows(NullPointerException.class, builder::build);
    }
    {
      GcpPubSubQueue.Builder builder =
          new Builder().projectId("abc").topicId("def").subscriptionId("");
      Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    }
  }
}
