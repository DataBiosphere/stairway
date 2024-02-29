package bio.terra.stairway.gcp;

import bio.terra.stairway.QueueInterface;
import bio.terra.stairway.QueueProcessFunction;
import bio.terra.stairway.exception.StairwayExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpPubSubQueue implements QueueInterface {
  private static final Logger logger = LoggerFactory.getLogger(GcpPubSubQueue.class);

  // Stairway expects that the only traffic in this queue is its own messages.
  // This byte limit is super-generous for what we currently use.
  private static final int MAX_INBOUND_MESSAGE_BYTES = 10000;

  private Publisher publisher;
  private final String subscriptionName;
  private SubscriberStub subscriberStub;

  // Get a builder to make the queue
  public static GcpPubSubQueue.Builder newBuilder() {
    return new GcpPubSubQueue.Builder();
  }

  /**
   * Construct the queue by creating a creating a publisher object for writing to the queue and a
   * subscriber for reading from the queue.
   *
   * @param builder the builder used to pass parameters
   * @throws IOException thrown if the publisher or subscriber cannot be created
   */
  public GcpPubSubQueue(GcpPubSubQueue.Builder builder) throws IOException {
    subscriptionName = ProjectSubscriptionName.format(builder.projectId, builder.subscriptionId);

    // Setup the publisher
    ProjectTopicName topicName = ProjectTopicName.of(builder.projectId, builder.topicId);
    publisher = Publisher.newBuilder(topicName).build();

    // Build the stub for issuing pulls from the queue
    SubscriberStubSettings subscriberStubSettings =
        SubscriberStubSettings.newBuilder()
            .setTransportChannelProvider(
                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                    .setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                    .build())
            .build();
    subscriberStub = GrpcSubscriberStub.create(subscriberStubSettings);
  }

  /**
   * Shutdown the queue The GCP interface is asymmetric. The subscriber is closable. The publisher
   * is shutdown.
   */
  public void shutdown() {
    if (subscriberStub != null) {
      subscriberStub.close();
      subscriberStub = null;
    }
    if (publisher != null) {
      publisher.shutdown();
      publisher = null;
    }
  }

  private PullResponse pullFromQueue(int numOfMessages, boolean returnImmediately) {
    PullRequest pullRequest =
        PullRequest.newBuilder()
            .setMaxMessages(numOfMessages)
            .setReturnImmediately(returnImmediately)
            .setSubscription(subscriptionName)
            .build();

    // use pullCallable().futureCall to asynchronously perform this operation
    // The call can complete without returning any messages.
    PullResponse pullResponse = null;
    try {
      pullResponse = subscriberStub.pullCallable().call(pullRequest);
      if (pullResponse.getReceivedMessagesList().size() == 0) {
        return null;
      }
    } catch (DeadlineExceededException ex) {
      // This error can happen when there are no messages in the queue
      logger.info("Deadline exceeded on pull request", ex);
    } catch (Exception ex) {
      // TODO: Is this the right error handling for this case?
      logger.warn("Unexpected exception on pull request - continuing", ex);
    }

    return pullResponse;
  }

  /**
   * Read and process queue messages
   *
   * @param numOfMessages number of messages to try to process in one go
   * @param processFunction Function to call for each message. The function returns true if the
   *     message is successfully handled; false if it was not and should remain on the queue.
   * @throws InterruptedException wait for messages is interrupted during Stairway shutdown
   */
  @Override
  public void dispatchMessages(int numOfMessages, QueueProcessFunction processFunction)
      throws InterruptedException {
    try {
      PullResponse pullResponse = pullFromQueue(numOfMessages, false);
      if (pullResponse == null) {
        return;
      }

      List<String> ackIds = new ArrayList<>();
      for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
        String smess = message.getMessage().getData().toStringUtf8();
        logger.info("Received message: " + smess);

        boolean processSucceeded = processFunction.apply(smess);
        if (processSucceeded) {
          ackIds.add(message.getAckId());
        }
      }

      if (ackIds.size() > 0) {
        // acknowledge received messages
        AcknowledgeRequest acknowledgeRequest =
            AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAllAckIds(ackIds)
                .build();
        // use acknowledgeCallable().futureCall to asynchronously perform this operation
        subscriberStub.acknowledgeCallable().call(acknowledgeRequest);
      }
    } catch (InterruptedException ex) {
      // Propagate InterruptedException. Otherwise, log the error and keep going.
      throw ex;
    } catch (Exception ex) {
      logger.warn("Unexpected exception dispatching messages - continuing", ex);
    }
  }

  /**
   * Put a message into the queue
   *
   * @param message the message to enqueue
   * @throws InterruptedException is possible from waiting on the pubsub enqueue
   */
  @Override
  public void enqueueMessage(String message) throws InterruptedException {
    ByteString data = ByteString.copyFromUtf8(message);
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

    try {
      // Once published, returns a server-assigned message id (unique within the topic)
      ApiFuture<String> future = publisher.publish(pubsubMessage);
      String messageId = future.get();
      logger.info("Queued message. Id: " + messageId + "; Msg: " + message);
    } catch (ExecutionException ex) {
      throw new StairwayExecutionException("Publish message failed", ex);
    }
  }

  /**
   * In tests, we often reuse the same pubsub queue. We want to clean the queue between tests. This
   * method is used to remove all messages from the queue.
   */
  @Override
  public void purgeQueueForTesting() {
    // Sometimes we get an empty response even when there are messages, so receive empty twice
    // before calling it purged.
    int emptyCount = 0;
    while (emptyCount < 2) {
      PullResponse pullResponse = pullFromQueue(1, true);
      if (pullResponse == null || pullResponse.getReceivedMessagesList().size() == 0) {
        emptyCount++;
        continue;
      }

      for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
        String smess = message.getMessage().getData().toStringUtf8();
        logger.info("Purging message: " + smess);

        AcknowledgeRequest acknowledgeRequest =
            AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAckIds(message.getAckId())
                .build();
        subscriberStub.acknowledgeCallable().call(acknowledgeRequest);
      }
    }
  }

  /** Use this builder class to create the GcpPubSubQueue. */
  public static class Builder {
    private String projectId;
    private String topicId;
    private String subscriptionId;

    /**
     * @param projectId GCP Project holding the topic and subscription
     * @return this
     */
    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * @param topicId identifier of the PubSub topic to enqueue messages
     * @return this
     */
    public Builder topicId(String topicId) {
      this.topicId = topicId;
      return this;
    }

    /**
     * @param subscriptionId identifier of the PubSub subscription read messages
     * @return this
     */
    public Builder subscriptionId(String subscriptionId) {
      this.subscriptionId = subscriptionId;
      return this;
    }

    public GcpPubSubQueue build() throws IOException {
      Validate.notEmpty(projectId, "A projectId is required");
      Validate.notEmpty(topicId, "A topicId is required");
      Validate.notEmpty(subscriptionId, "A subscriptionId is required");
      return new GcpPubSubQueue(this);
    }
  }
}
