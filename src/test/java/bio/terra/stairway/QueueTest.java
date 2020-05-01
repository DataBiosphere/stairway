package bio.terra.stairway;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("connected")
public class QueueTest {
    private Logger logger = LoggerFactory.getLogger(QueueTest.class);

    private static final String subscriptionId = "queueTest-sub";
    private static final String topicId = "queueTest-queue";

    private Queue workQueue;

    @BeforeEach
    public void setup() throws Exception {
        String projectId = TestUtil.getEnvVar("GOOGLE_CLOUD_PROJECT", null);
        if (projectId == null) {
            throw new IllegalStateException("You must have GOOGLE_CLOUD_PROJECT and " +
                    "GOOGLE_APPLICATION_CREDENTIALS envvars defined");
        }
        workQueue = new Queue(null, projectId, subscriptionId, topicId);
        workQueue.purgeQueue();
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

        workQueue.queueMessage("msg1");
        workQueue.queueMessage("msg2");
        workQueue.queueMessage("msg3");

        workQueue.dispatchMessages(2, this::simpleMessageProcessor);

        workQueue.queueMessage("msg4");
        workQueue.queueMessage("msg5");

        workQueue.dispatchMessages(2, this::simpleMessageProcessor);
        workQueue.dispatchMessages(2, this::simpleMessageProcessor);

        for (Map.Entry<String, Boolean> entry : messages.entrySet()) {
            Boolean seen = (Boolean) entry.getValue();
            String message = (String) entry.getKey();
            logger.info("Saw value for " + message);
            assertTrue(seen, "saw value for " + message);
        }
    }

    @Test
    public void deleteTest() throws Exception {
        workQueue.deleteQueue();
    }

    public Boolean simpleMessageProcessor(String message, Stairway stairway) {
        logger.info("Dispatched " + message);
        messages.replace(message, true);
        return true;
    }
}
