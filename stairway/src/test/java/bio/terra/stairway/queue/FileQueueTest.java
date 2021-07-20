package bio.terra.stairway.queue;

import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.fixtures.FileQueue;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test to make sure the FileQueue fixture works properly. Kinda meta... */
@Tag("unit")
public class FileQueueTest {
  private final Logger logger = LoggerFactory.getLogger(FileQueueTest.class);
  private FileQueue workQueue;

  @BeforeEach
  public void setup() {
    workQueue = FileQueue.makeFileQueue("filequeuetest");
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
}
