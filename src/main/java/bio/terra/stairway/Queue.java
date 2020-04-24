package bio.terra.stairway;

// For starters, let's just code for pubsub. When the API seems right, we can push it down into a Google package
// and lay an interface on top.
//
// I created a topic via GCP console called stairway-queue
// I created a subscription via GCP console called stairway-queue-sub


import bio.terra.stairway.exception.StairwayExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * ServiceQueue implements a service-wide queue for Stairway. The initial implementation is
 * only for GCP. When we have more than one cloud, we can make this an interface and push
 * implementations down to cloud-specific packages.
 */
public class Queue {
    private static final Logger logger = LoggerFactory.getLogger(Queue.class);

    // -- Queue Parameters --
    // These parameters are used when constructing the queue and the subscriber. I don't think they need
    // to be settable, but we may want to tune them as we get experience with the queue.
    private static final int MAX_INBOUND_MESSAGE_BYTES = 10000;
    private static final long MESSAGE_RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(3);
    private static final int ACK_DEADLINE_SECONDS = 100;

    private final String projectId;
    private final String subscriptionId;
    private final String topicId;
    private final String subscriptionName;
    private final Stairway stairway;
    private final Publisher publisher;
    private SubscriberStub subscriber;

    public Queue(Stairway stairway, String projectId, String subscriptionId, String topicId) throws IOException {
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.topicId = topicId;

        // Create the topic and subscription
        maybeCreateTopicAndSubscription();

        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                        SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                .setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                                .build())
                .build();
        subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
        subscriber = GrpcSubscriberStub.create(subscriberStubSettings);

        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        publisher = Publisher.newBuilder(topicName).build();

        this.stairway = stairway;
    }

    // Create a topic and a subscription, if it doesn't exist
    private void maybeCreateTopicAndSubscription()
            throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.createTopic(topicName);
            logger.info("Created topic: " + topicId);
        } catch (AlreadyExistsException ex) {
            logger.info("Topic already exists: " + topicId);
        }

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            Subscription request = Subscription.newBuilder()
                    .setName(subscriptionName.toString())
                    .setTopic(topicName.toString())
                    .setAckDeadlineSeconds(ACK_DEADLINE_SECONDS)
                    .setMessageRetentionDuration(Duration.newBuilder().setSeconds(MESSAGE_RETENTION_SECONDS))
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
        if (subscriber != null) {
            subscriber.close();
            subscriber = null;
        }
    }

    public void shutdown() {
        shutdownSubscriber();
        if (publisher != null) {
            publisher.shutdown();
        }
    }

    public void dispatchMessages(int numOfMessages, QueueProcessFunction<String, Stairway, Boolean> processFunction)
            throws InterruptedException {

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(numOfMessages)
                        .setReturnImmediately(false)
                        .setSubscription(subscriptionName)
                        .build();

        // use pullCallable().futureCall to asynchronously perform this operation
        // The call can complete without returning any messages.
        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
        if (pullResponse.getReceivedMessagesList().size() == 0) {
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
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
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

    public void purgeQueue() {
        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(1)
                        .setReturnImmediately(true)
                        .setSubscription(subscriptionName)
                        .build();

        // Sometimes we get an empty response even when there are messages, so receive empty twice
        // before calling it purged.
        int emptyCount = 0;
        while (emptyCount < 2) {
            PullResponse pullResponse;
            pullResponse = subscriber.pullCallable().call(pullRequest);

            if (pullResponse.getReceivedMessagesList().size() == 0) {
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
                subscriber.acknowledgeCallable().call(acknowledgeRequest);
            }
        }
    }

    public void deleteQueue() {
        TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.deleteTopic(topicName);
        } catch (IOException ex) {
            logger.warn("Failed to delete topic: " + topicName);
        }

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subscriptionId);
            subscriptionAdminClient.deleteSubscription(subscription);
        } catch (IOException ex) {
            logger.warn("Failed to delete subscription: " + subscriptionName);
        }
    }
}
