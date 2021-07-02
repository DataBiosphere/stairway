package bio.terra.stairway.gcp;

import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for creating PubSub topics and subscriptions.
 * The default credentials must have {@code roles/pubsub.editor} or
 * {@code roles/editor} on the project in order to use these methods.
 *
 * It is not necessary to use these to use GcpPubSubQueue. In fact,
 * it is best to create these object in Terraform or similar and not at run time,
 * so that the running instance does not need to hold the enhanced permissions.
 */
@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
    justification = "Spotbugs doesn't understand resource try construct")
public class GcpQueueUtils {
  private static final Logger logger = LoggerFactory.getLogger(GcpQueueUtils.class);

  // Default values used when making the Gcp PubSub subscription
  private static final long MESSAGE_RETENTION_SECONDS = TimeUnit.DAYS.toSeconds(3);
  private static final int ACK_DEADLINE_SECONDS = 100;

  /**
   * Create a topic in a project
   *
   * @param projectId project in which the topic should be created
   * @param topicId name of the topic to create
   * @throws IOException error thrown when the create doesn't work
   */
  public static void makeTopic(String projectId, String topicId) throws IOException {
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

    GoogleCredentialsProvider credentialsProvider =
        GoogleCredentialsProvider.newBuilder()
            .setScopesToApply(Collections.singletonList("https://www.googleapis.com/auth/pubsub"))
            .build();
    TopicAdminSettings topicAdminSettings =
        TopicAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
      topicAdminClient.createTopic(topicName);
    }
  }

  /**
   * Create a subscription for a topic in a project using default parameters
   *
   * @param projectId project in which the subscription should be created
   * @param topicId topic the subscription is created for
   * @param subscriptionId name of the subscription to create
   * @throws IOException error thrown when the create doesn't work
   */
  public static void makeSubscription(String projectId, String topicId, String subscriptionId)
        throws IOException {
    makeSubscription(projectId, topicId, subscriptionId, MESSAGE_RETENTION_SECONDS, ACK_DEADLINE_SECONDS);
  }

  /**
   * Create a subscription for a topic specifying all parameters
   *
   * @param projectId project in which the subscription should be created
   * @param topicId topic the subscription is created for
   * @param subscriptionId name of the subscription to create
   * @param messageRetentionSeconds seconds to retain the message in the queue
   * @param ackDeadlineSeconds seconds Stairway has to handle the message
   * @throws IOException error thrown when the create doesn't work
   */
  public static void makeSubscription(
      String projectId,
      String topicId,
      String subscriptionId,
      long messageRetentionSeconds,
      int ackDeadlineSeconds) throws IOException {

    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

    GoogleCredentialsProvider credentialsProvider =
        GoogleCredentialsProvider.newBuilder()
            .setScopesToApply(Collections.singletonList("https://www.googleapis.com/auth/pubsub"))
            .build();

    SubscriptionAdminSettings subscriptionAdminSettings =
        SubscriptionAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();

    try (SubscriptionAdminClient subscriptionAdminClient =
        SubscriptionAdminClient.create(subscriptionAdminSettings)) {
      Subscription request =
          Subscription.newBuilder()
              .setName(subscriptionName.toString())
              .setTopic(topicName.toString())
              .setAckDeadlineSeconds(ackDeadlineSeconds)
              .setMessageRetentionDuration(Duration.newBuilder().setSeconds(messageRetentionSeconds))
              .build();

      subscriptionAdminClient.createSubscription(request);
    }
  }

  /**
   * Delete the topic and subscription used for the queue
   *
   * @param projectId project where the topic and subscription live
   * @param topicId identifier of the topic to delete
   * @param subscriptionId identifier of the subscription to delete
   * @throws IOException error thrown when one of the deletes does not work
   */
  public static void deleteQueue(String projectId, String topicId, String subscriptionId)
      throws IOException {
    TopicName topicName = TopicName.ofProjectTopicName(projectId, topicId);

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      topicAdminClient.deleteTopic(topicName);
    }

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subscriptionId);
      subscriptionAdminClient.deleteSubscription(subscription);
    }
  }
}
