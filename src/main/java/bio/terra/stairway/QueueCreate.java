package bio.terra.stairway;

// In most cases we expect callers to provide case subscription and a topic. For backward
// compatibility and
// testing situation stairways able to create subscriptions and topics itself. This module
// encapsulates
// the creation operations.

import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
public class QueueCreate {
  private static final Logger logger = LoggerFactory.getLogger(QueueCreate.class);

  // -- Queue Parameters --
  // These parameters are used when constructing the queue and the subscriber. I don't think they
  // need to be settable, but we may want to tune them as we get experience with the queue.
  private static final int MAX_INBOUND_MESSAGE_BYTES = 10000;
  private static final long MESSAGE_RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(3);
  private static final int ACK_DEADLINE_SECONDS = 100;

  public static void makeTopic(String projectId, String topicId) throws IOException {
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

    logger.debug("Construct credentials");
    GoogleCredentialsProvider credentialsProvider =
        GoogleCredentialsProvider.newBuilder()
            .setScopesToApply(Collections.singletonList("https://www.googleapis.com/auth/pubsub"))
            .build();
    TopicAdminSettings topicAdminSettings =
        TopicAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();

    logger.debug("Try to create the topic");
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
      topicAdminClient.createTopic(topicName);
      logger.debug("Created topic: " + topicId);
    } catch (AlreadyExistsException ex) {
      logger.debug("Topic already exists: " + topicId);
    }
  }

  public static void makeSubscription(String projectId, String topicId, String subscriptionId)
      throws IOException {
    logger.debug("Construct names");
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);
    ProjectSubscriptionName subscriptionName =
        ProjectSubscriptionName.of(projectId, subscriptionId);

    logger.debug("Construct credentials");
    GoogleCredentialsProvider credentialsProvider =
        GoogleCredentialsProvider.newBuilder()
            .setScopesToApply(Collections.singletonList("https://www.googleapis.com/auth/pubsub"))
            .build();

    SubscriptionAdminSettings subscriptionAdminSettings =
        SubscriptionAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();

    logger.debug("Try to create the subscription");
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
      logger.debug("Created subscription: " + subscriptionId);
    } catch (AlreadyExistsException ex) {
      logger.debug("Subscription already exists: " + subscriptionId);
    }
  }

  public static void deleteQueue(String projectId, String topicId, String subscriptionId) {
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
      logger.warn("Failed to delete subscription: " + subscriptionId, ex);
    }
  }
}
