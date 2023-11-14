package bio.terra.stairway.azure;

import bio.terra.stairway.QueueInterface;
import bio.terra.stairway.QueueProcessFunction;
import bio.terra.stairway.exception.StairwayExecutionException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureServiceBusQueue implements QueueInterface {

  private static final Logger logger = LoggerFactory.getLogger(AzureServiceBusQueue.class);

  private final ServiceBusReceiverClient serviceBusReceiverClient;
  private final ServiceBusSenderClient serviceBusSenderClient;

  public static AzureServiceBusQueue.Builder newBuilder() {
    return new AzureServiceBusQueue.Builder();
  }

  /**
   * Construct the queue by creating a receiver and sender client for reading and writing to the
   * subscription and reading from the topic.
   *
   * @param serviceBusReceiverClient the receiver client
   * @param serviceBusSenderClient the sender client
   */
  AzureServiceBusQueue(
      ServiceBusReceiverClient serviceBusReceiverClient,
      ServiceBusSenderClient serviceBusSenderClient) {
    this.serviceBusReceiverClient = serviceBusReceiverClient;
    this.serviceBusSenderClient = serviceBusSenderClient;
  }

  /**
   * Construct the queue by creating a receiver and sender client using the builder parameters.
   *
   * @param builder the builder used to pass parameters
   */
  public AzureServiceBusQueue(AzureServiceBusQueue.Builder builder) {
    ServiceBusClientBuilder serviceBusClientBuilder;

    if (builder.useManagedIdentity) {
      serviceBusClientBuilder =
          new ServiceBusClientBuilder()
              .fullyQualifiedNamespace(builder.namespace)
              .credential(new DefaultAzureCredentialBuilder().build());
    } else {
      serviceBusClientBuilder =
          new ServiceBusClientBuilder().connectionString(builder.connectionString);
    }

    serviceBusReceiverClient =
        serviceBusClientBuilder
            .receiver()
            .topicName(builder.topicName)
            .subscriptionName(builder.subscriptionName)
            .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
            .disableAutoComplete()
            .maxAutoLockRenewDuration(builder.maxAutoLockRenewDuration)
            .buildClient();

    serviceBusSenderClient =
        serviceBusClientBuilder.sender().topicName(builder.topicName).buildClient();
  }

  /**
   * Reads a message from Service Bus using the receiver client. The receive operation uses
   * peek-lock semantics. The message is acknowledged after it has been successfully processed. If
   * the message fails to be processed, the message will be abandoned (placed back on the
   * queue/subscription).
   */
  @Override
  public void dispatchMessages(int maxMessages, QueueProcessFunction processFunction)
      throws InterruptedException {

    // Iterating over a list instead of a stream().forEach() so that the interrupted exception can
    // be re-thrown
    List<ServiceBusReceivedMessage> messages =
        serviceBusReceiverClient.receiveMessages(maxMessages).stream().toList();

    for (ServiceBusReceivedMessage message : messages) {
      ProcessMessage(processFunction, message);
    }
  }

  private void ProcessMessage(
      QueueProcessFunction processFunction, ServiceBusReceivedMessage message)
      throws InterruptedException {
    try {
      // toString uses StandardCharsets.UTF_8 by default
      boolean processSucceeded = processFunction.apply(message.getBody().toString());

      if (processSucceeded) {
        serviceBusReceiverClient.complete(message);
        logger.info("Completed message. ID: {}", message.getMessageId());
        return;
      }

      logger.info("Failed to process message. ID: {}", message.getMessageId());
      serviceBusReceiverClient.abandon(message);

    } catch (InterruptedException ex) {
      logger.error("InterruptedException was thrown. Processing will stop", ex);
      serviceBusReceiverClient.abandon(message);
      throw ex;
    } catch (Exception ex) {
      logger.warn("Unexpected exception dispatching or processing messages - continuing", ex);
      serviceBusReceiverClient.abandon(message);
    }
  }

  /**
   * Enqueues a message to Service Bus using the sender client.
   *
   * @param message the message to enqueue
   * @throws StairwayExecutionException if an unexpected Service Bus exception occurs
   */
  @Override
  public void enqueueMessage(String message) throws StairwayExecutionException {
    try {
      ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message);
      serviceBusSenderClient.sendMessage(serviceBusMessage);
      logger.info("Successfully sent message. ID: {}", serviceBusMessage.getMessageId());
    } catch (ServiceBusException ex) {
      logger.error("Unexpected exception sending the message via Azure Service Bus", ex);
      throw new StairwayExecutionException(
          "Unexpected exception sending the message via Azure Service Bus", ex);
    }
  }

  @Override
  public void purgeQueueForTesting() {
    throw new NotImplementedException(
        "purgeQueueForTesting is not implemented for Azure Service Bus");
  }

  /** Use this builder class to create the AzurePubSubQueue. */
  public static class Builder {
    public Duration maxAutoLockRenewDuration = Duration.ofMinutes(15);
    private String connectionString;
    private boolean useManagedIdentity;
    private String namespace;
    private String topicName;
    private String subscriptionName;

    /**
     * Set the maximum duration for which the lock on each message will be renewed automatically.
     */
    public Builder maxAutoLockRenewDuration(Duration maxAutoLockRenewDuration) {
      this.maxAutoLockRenewDuration = maxAutoLockRenewDuration;
      return this;
    }

    /**
     * Set the connection string for the Azure Service Bus namespace.
     *
     * @param connectionString the connection string
     * @return this
     */
    public Builder connectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    /**
     * Set the flag to use managed identity for Azure Service Bus. If this is set, the namespace
     * must be set.
     *
     * @param useManagedIdentity the flag to use managed identity
     * @return this
     */
    public Builder useManagedIdentity(boolean useManagedIdentity) {
      this.useManagedIdentity = useManagedIdentity;
      return this;
    }

    /**
     * Set the namespace for the Azure Service Bus. Required if using managed identity.
     *
     * @param namespace the namespace
     * @return this
     */
    public Builder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Set the topic name where messages will be sent.
     *
     * @param topicName the topic name
     * @return this
     */
    public Builder topicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    /**
     * Set the subscription name where messages will be received.
     *
     * @param subscriptionName the subscription name
     * @return this
     */
    public Builder subscriptionName(String subscriptionName) {
      this.subscriptionName = subscriptionName;
      return this;
    }

    public AzureServiceBusQueue build() throws IllegalArgumentException, NullPointerException {
      Validate.notEmpty(subscriptionName, "A subscriptionName is required");
      Validate.notEmpty(topicName, "A topicName is required");
      Validate.inclusiveBetween(
          Duration.ofSeconds(10),
          Duration.ofMinutes(30),
          maxAutoLockRenewDuration,
          "maxAutoLockRenewDuration must be between 10 seconds and 30 minutes");

      if (useManagedIdentity) {
        Validate.notEmpty(namespace, "A namespace is required");
        return new AzureServiceBusQueue(this);
      }

      Validate.notEmpty(connectionString, "A connectionString is required");
      return new AzureServiceBusQueue(this);
    }
  }
}
