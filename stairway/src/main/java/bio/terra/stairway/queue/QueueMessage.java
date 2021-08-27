package bio.terra.stairway.queue;

import bio.terra.stairway.impl.StairwayImpl;

abstract class QueueMessage {
  static final String FORMAT_VERSION = "1";

  abstract boolean process(StairwayImpl stairway) throws InterruptedException;
}
