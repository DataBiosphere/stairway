package bio.terra.stairway.queue;

import bio.terra.stairway.impl.StairwayImpl;

/**
 * QueueMessage is a base class for messages put in the Stairway work queue. It defines the format
 * version number of the message. If the format changes, that will need to cascade through the
 * subclasses.
 */
abstract class QueueMessage {
  static final String FORMAT_VERSION = "1";

  abstract boolean process(StairwayImpl stairway) throws InterruptedException;
}
