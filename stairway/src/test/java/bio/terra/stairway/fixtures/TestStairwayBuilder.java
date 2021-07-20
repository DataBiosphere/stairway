package bio.terra.stairway.fixtures;

import static bio.terra.stairway.fixtures.TestUtil.makeDataSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.QueueInterface;
import bio.terra.stairway.ShortUUID;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StairwayBuilder;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

/** Builder class to create stairways for testing */
public class TestStairwayBuilder {
  public enum UseQueue {
    NO_QUEUE, // no queue for this stairway
    MAKE_QUEUE, // create a new queue for this stairway; the queue name is random
    USE_QUEUE // use the provided queue for this stairway (implicit if a queue is provided)
  }

  private String name;
  private UseQueue useQueue;
  private QueueInterface workQueue;
  private boolean continuing;
  private int testHookCount;
  private boolean doRecoveryCheck;
  private String flightId;

  /** Set stairway name. If not present, a random name is generated */
  public TestStairwayBuilder name(String name) {
    this.name = name;
    return this;
  }

  /** Control how queuing is done. Defaults to NO_QUEUE. */
  public TestStairwayBuilder useQueue(UseQueue useQueue) {
    this.useQueue = useQueue;
    return this;
  }

  /** If a workQueue is set, useQueue is implicitly set to USE_QUEUE. */
  public TestStairwayBuilder workQueue(QueueInterface workQueue) {
    this.workQueue = workQueue;
    return this;
  }

  /** If true, then we do not initialize the database, so we will recover flights. Default false */
  public TestStairwayBuilder continuing(boolean continuing) {
    this.continuing = continuing;
    return this;
  }

  /** Number of TestHooks to make */
  public TestStairwayBuilder testHookCount(int testHookCount) {
    this.testHookCount = testHookCount;
    return this;
  }

  /**
   * The recovery check verifies that there is a single stairway name on the recovery list and it
   * matches the incoming stairway name. This is to support shutdown and recovery testing. Defaults
   * to false.
   */
  public TestStairwayBuilder doRecoveryCheck(boolean doRecoveryCheck) {
    this.doRecoveryCheck = doRecoveryCheck;
    return this;
  }

  /**
   * If doRecoveryCheck is true and this is specified, test that this flight is in the READY state
   * prior to running recovery. Defaults to null;
   */
  public TestStairwayBuilder flightId(String flightId) {
    this.flightId = flightId;
    return this;
  }

  /** build, initialize, and recover the stairway instance */
  public Stairway build() throws Exception {
    // Set default values
    String buildName = Optional.ofNullable(name).orElse("test_" + ShortUUID.get());
    UseQueue buildUseQueue = Optional.ofNullable(useQueue).orElse(UseQueue.NO_QUEUE);
    if (workQueue != null) {
      buildUseQueue = UseQueue.USE_QUEUE;
    }

    // Setup queue as needed
    QueueInterface buildWorkQueue = null;
    switch (buildUseQueue) {
      case NO_QUEUE:
        break;
      case MAKE_QUEUE:
        String queueName = "testq_" + ShortUUID.get();
        buildWorkQueue = FileQueue.makeFileQueue(queueName);
        break;
      case USE_QUEUE:
        buildWorkQueue = workQueue;
        break;
    }

    StairwayBuilder builder =
        new StairwayBuilder()
            .stairwayName(buildName)
            .maxParallelFlights(2)
            .workQueue(buildWorkQueue);

    for (int i = 0; i < testHookCount; i++) {
      int hookId = i + 1;
      TestHook hook = new TestHook(String.valueOf(hookId));
      builder.stairwayHook(hook);
    }
    if (!continuing) {
      // If we are continuing a test, then don't clear the hook log
      TestHook.clearHookLog();
    }

    Stairway stairway = builder.build();

    DataSource dataSource = makeDataSource();
    List<String> recordedStairways = stairway.initialize(dataSource, !continuing, !continuing);

    // Validate the recordedStairways list
    if (!continuing) {
      assertThat("nothing to recover", recordedStairways.size(), equalTo(0));
    } else if (doRecoveryCheck) {
      assertThat("one stairway to recover", recordedStairways.size(), equalTo(1));
      assertThat("stairway name matches", recordedStairways.get(0), equalTo(name));

      if (flightId != null) {
        FlightState state = stairway.getFlightState(flightId);
        assertThat("State is ready", state.getFlightStatus(), equalTo(FlightStatus.READY));
        assertNull(state.getStairwayId(), "Flight is unowned");
      }
    }

    stairway.recoverAndStart(recordedStairways);
    return stairway;
  }
}
