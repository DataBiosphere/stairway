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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
class GcpPubSubQueue implements QueueInterface {
  private static final Logger logger = LoggerFactory.getLogger(GcpPubSubQueue.class);

  // Stairway expects that the only traffic in this queue is its own messages.
  // This byte limit is super-generous for what we currently use.
  private static final int MAX_INBOUND_MESSAGE_BYTES = 10000;

  private final Publisher publisher;
  private final String subscriptionName;
  private SubscriberStub subscriberStub;

  // Get a builder to make the queue
  public static GcpPubSubQueue.Builder newBuilder() {
    return new GcpPubSubQueue.Builder();
  }

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

  public void shutdown() {
    if (subscriberStub != null) {
      subscriberStub.close();
      subscriberStub = null;
    }
    if (publisher != null) {
      publisher.shutdown();
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

  @Override
  public void dispatchMessages(
      Object dispatchContext, int numOfMessages, QueueProcessFunction processFunction)
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

        boolean processSucceeded = processFunction.apply(smess, dispatchContext);
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

  @Override
  public void purgeQueue() {
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
