package bio.terra.stairway.gcp;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test to make sure the GcpPubSubQueue is basically working */
@Tag("connected")
public class PubSubQueueTest {
  private final Logger logger = LoggerFactory.getLogger(PubSubQueueTest.class);

  private static final String subscriptionId = "queueTest-sub";
  private static final String topicId = "queueTest-queue";

  private GcpPubSubQueue workQueue;
  private String projectId;

  @BeforeEach
  public void setup() throws Exception {
    projectId = getProjectId();
    GcpQueueUtils.makeTopic(projectId, topicId);
    GcpQueueUtils.makeSubscription(projectId, topicId, subscriptionId);
    workQueue =
        GcpPubSubQueue.newBuilder()
            .projectId(projectId)
            .topicId(topicId)
            .subscriptionId(subscriptionId)
            .build();
    workQueue.purgeQueue();
  }

  @AfterEach
  public void teardown() throws Exception {
    workQueue.shutdown();
    GcpQueueUtils.deleteQueue(projectId, topicId, subscriptionId);
  }

  private Map<String, Boolean> messages;

  @Test
  public void simpleTest() throws Exception {
    messages = new HashMap<>();
    messages.put("msg1", false);
    messages.put("msg2", false);
    messages.put("msg3", false);
    messages.put("msg4", false);
    messages.put("msg5", false);

    workQueue.enqueueMessage("msg1");
    workQueue.enqueueMessage("msg2");
    workQueue.enqueueMessage("msg3");

    workQueue.dispatchMessages(null, 2, this::simpleMessageProcessor);

    workQueue.enqueueMessage("msg4");
    workQueue.enqueueMessage("msg5");

    workQueue.dispatchMessages(null, 2, this::simpleMessageProcessor);
    workQueue.dispatchMessages(null, 2, this::simpleMessageProcessor);

    for (Map.Entry<String, Boolean> entry : messages.entrySet()) {
      Boolean seen = entry.getValue();
      String message = entry.getKey();
      logger.info("Saw value for " + message);
      assertTrue(seen, "saw value for " + message);
    }
  }

  public Boolean simpleMessageProcessor(String message, Object dispatchContext) {
    logger.info("Dispatched " + message);
    messages.replace(message, true);
    return true;
  }

  /**
   * Get the project id from the environment. If it is not set we bail. This is used in connected
   * tests to make sure we are connected.
   *
   * @return projectId
   */
  public static String getProjectId() {
    String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (projectId == null) {
      throw new IllegalStateException("You must have GOOGLE_CLOUD_PROJECT defined");
    }
    return projectId;
  }
}
