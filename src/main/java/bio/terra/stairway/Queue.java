package bio.terra.stairway;

// For starters, let's just code for pubsub. When the API seems right, we can push it down into a
// Google package
// and lay an interface on top.
//
// I created a topic via GCP console called stairway-queue
// I created a subscription via GCP console called stairway-queue-sub

import bio.terra.stairway.exception.StairwayExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServiceQueue implements a service-wide queue for Stairway. The initial implementation is only for
 * GCP. When we have more than one cloud, we can make this an interface and push implementations
 * down to cloud-specific packages.
 */
@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
public class Queue {
  private static final Logger logger = LoggerFactory.getLogger(Queue.class);

  // -- Queue Parameters --
  // These parameters are used when constructing the queue and the subscriber. I don't think they
  // need
  // to be settable, but we may want to tune them as we get experience with the queue.
  private static final int MAX_INBOUND_MESSAGE_BYTES = 10000;
  private static final long MESSAGE_RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(3);
  private static final int ACK_DEADLINE_SECONDS = 100;

  private final String projectId;
  private final String subscriptionId;
  private final String topicId;

  private final Stairway stairway;
  private final Publisher publisher;
  private final String subscriptionName;
  private SubscriberStub subscriberStub;

  public Queue(Stairway stairway, String projectId, String topicId, String subscriptionId)
      throws IOException {
    this.projectId = projectId;
    this.topicId = topicId;
    this.subscriptionId = subscriptionId;
    subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);

    // Create the topic and subscription
    maybeCreateTopicAndSubscription();

    // Setup the publisher
    ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
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

    this.stairway = stairway;
  }

  // Create a topic and a subscription, if it doesn't exist
  private void maybeCreateTopicAndSubscription() throws IOException {
    logger.info("Start maybeCreateTopicAndSubscription");
    ProjectSubscriptionName subscriptionName =
        ProjectSubscriptionName.of(projectId, subscriptionId);
    logger.info("Construct topic name");
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

    logger.info("Construct credentials");
    GoogleCredentialsProvider credentialsProvider =
        GoogleCredentialsProvider.newBuilder()
            .setScopesToApply(Collections.singletonList("https://www.googleapis.com/auth/pubsub"))
            .build();
    logger.info("Credentials are: " + credentialsProvider);
    TopicAdminSettings topicAdminSettings =
        TopicAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();
    SubscriptionAdminSettings subscriptionAdminSettings =
        SubscriptionAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();

    logger.info("Try to create the topic");
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
      topicAdminClient.createTopic(topicName);
      logger.info("Created topic: " + topicId);
    } catch (AlreadyExistsException ex) {
      logger.info("Topic already exists: " + topicId);
    }

    logger.info("Try to create the subscription");
    try (SubscriptionAdminClient subscriptionAdminClient =
        SubscriptionAdminClient.create(subscriptionAdminSettings)) {
      Subscription request =
          Subscription.newBuilder()
              .setName(subscriptionName.toString())
              .setTopic(topicName.toString())
              .setAckDeadlineSeconds(ACK_DEADLINE_SECONDS)
              .setMessageRetentionDuration(
                  Duration.newBuilder().setSeconds(MESSAGE_RETENTION_SECONDS))
              .build();

      subscriptionAdminClient.createSubscription(request);
      logger.info("Created subscription: " + subscriptionId);
    } catch (AlreadyExistsException ex) {
      logger.info("Subscription already exists: " + subscriptionId);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    shutdownSubscriber();
  }

  public void shutdownSubscriber() {
    if (subscriberStub != null) {
      subscriberStub.close();
      subscriberStub = null;
    }
  }

  public void shutdown() {
    shutdownSubscriber();
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

  public void dispatchMessages(
      int numOfMessages, QueueProcessFunction<String, Stairway, Boolean> processFunction)
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

        Boolean processSucceeded = processFunction.apply(smess, stairway);
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
      throw ex;
    } catch (Exception ex) {
      // TODO: Is this the right error handling?
      logger.warn("Unexpected exception dispatching messages - continuing", ex);
    }
  }

  public void queueMessage(String message) throws InterruptedException, StairwayExecutionException {
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

  // NOTE: this is only for use in controlled tests. It should not be used to empty a queue in
  // production.
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

  public void deleteQueue() {
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      topicAdminClient.deleteTopic(topicName);
    } catch (IOException ex) {
      logger.warn("Failed to delete topic: " + topicName, ex);
    }

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subscriptionId);
      subscriptionAdminClient.deleteSubscription(subscription);
    } catch (IOException ex) {
      logger.warn("Failed to delete subscription: " + subscriptionName, ex);
    }
  }
}
