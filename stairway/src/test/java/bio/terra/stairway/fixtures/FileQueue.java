package bio.terra.stairway.fixtures;

import bio.terra.stairway.QueueInterface;
import bio.terra.stairway.QueueProcessFunction;
import bio.terra.stairway.ShortUUID;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the QueueInterface as a temp directory on the local file system.
 * It is only intended for testing.
 *
 * Enqueue is writing a file containing the message into the directory. The file is
 * named as a UUID.
 *
 * Dispatching messages is reading the directory, sorting by last modified time,
 * and picking out the oldest file. If the processing function accepts the file,
 * we delete it.
 *
 * Purging the queue is `rm -f *` more or less.
 */
public class FileQueue implements QueueInterface {
  private static final Logger logger = LoggerFactory.getLogger(QueueInterface.class);

  /** Time to wait between polling the directory for new message files */
  private static final Duration DIRECTORY_POLL_WAIT = Duration.ofSeconds(2);

  /** Number of times to poll before giving up */
  private static final int DIRECTORY_POLL_COUNT = 5;

  private final File queueDir;

  public FileQueue(File queueDir) {
    this.queueDir = queueDir;
  }

  @Override
  public void dispatchMessages(Object dispatchContext, int maxMessages,
      QueueProcessFunction processFunction) throws InterruptedException {

    File[] files = waitForFiles();
    if (files == null) {
      // We didn't find any files after trying for awhile
      return;
    }
    Arrays.sort(files, Comparator.comparingLong(File::lastModified));

    for (int i = 0; (i < maxMessages && i < files.length); i++) {
      File msgFile = files[i];
      try {
        String message = FileUtils.readFileToString(msgFile, StandardCharsets.UTF_8);
        boolean handled = processFunction.apply(message, dispatchContext);
        if (handled) {
          boolean deleted = msgFile.delete();
          if (!deleted) {
            logger.info("Failed to delete " + msgFile.getPath());
          }
        }
      } catch (IOException e) {
        logger.info("Failure handling read message file " + msgFile.getPath(), e);
      }
    }
  }

  private File[] waitForFiles() throws InterruptedException {
    for (int i = 0; i < DIRECTORY_POLL_COUNT; i++) {
      File[] files = queueDir.listFiles();
      if (files == null || files.length == 0) {
        TimeUnit.SECONDS.sleep(DIRECTORY_POLL_WAIT.toSeconds());
      } else {
        return files;
      }
    }
    return null;
  }

  @Override
  public void enqueueMessage(String message) throws InterruptedException {
    File msgFile = new File(queueDir, "msg" + ShortUUID.get());
    try (FileWriter writer = new FileWriter(msgFile)) {
      writer.write(message);
    } catch (IOException e) {
      throw new StairwayExecutionException("enqueue error", e);
    }
  }

  @Override
  public void purgeQueue() {
    File[] files = queueDir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      boolean deleted = file.delete();
      if (!deleted) {
        logger.info("Failed to delete " + file.getPath());
      }
    }
  }

  // Factory for creating the queue directory and making a file queue using it
  // and purging the queue so it is clean for the test.
  public static FileQueue makeFileQueue(String name) {
    File queueDir = new File("/tmp/" + name);
    if (queueDir.mkdir()) {
      logger.info("Created queue directory: " + queueDir.getPath());
    } else {
      logger.info("Queue directory exists: " + queueDir.getPath());
    }
    FileQueue aQueue = new FileQueue(queueDir);
    aQueue.purgeQueue();
    return aQueue;
  }
}
