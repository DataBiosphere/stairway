package bio.terra.stairway.azure;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
class QueueBuilderTest {
  private AzureServiceBusQueue.Builder builder;
  private static final String SB_NAMESPACE = "foo.servicebus.windows.net";
  private static final String SB_CONN_STRING =
      "Endpoint=sb://foo.servicebus.windows.net/;SharedAccessKeyName=fortesting;SharedAccessKey=XXXXXXXXXXXX";

  @BeforeEach
  void setUp() {
    builder = AzureServiceBusQueue.newBuilder();
  }

  @Test
  void build_noSubscriptionOrTopic_throwsException() {

    assertThrows(NullPointerException.class, () -> builder.build());
  }

  // Test that the builder throws if managed identity is true and the namespace is missing
  @Test
  void build_managedIdentityTrueAndNamespaceMissing_throwsException() {
    assertThrows(
        NullPointerException.class,
        () ->
            builder
                .subscriptionName("subscriptionName")
                .topicName("topicName")
                .useManagedIdentity(true)
                .build());
  }

  // Test that the builder succeeds if managed identity is true and the namespace is provided
  @Test
  void build_managedIdentityTrueAndNamespaceProvided_succeeds() {
    builder
        .subscriptionName("subscriptionName")
        .topicName("topicName")
        .useManagedIdentity(true)
        .namespace(SB_NAMESPACE)
        .build();
  }

  // Test that the builder throws if managed identity is false and the connection string is missing
  @Test
  void build_managedIdentityFalseAndConnectionStringMissing_throwsException() {
    assertThrows(
        NullPointerException.class,
        () ->
            builder
                .subscriptionName("subscriptionName")
                .topicName("topicName")
                .useManagedIdentity(false)
                .build());
  }

  // Test that the builder succeeds if managed identity is false and the connection string is
  // provided
  @Test
  void build_managedIdentityFalseAndConnectionStringProvided_succeeds() {
    builder
        .subscriptionName("subscriptionName")
        .topicName("topicName")
        .useManagedIdentity(false)
        .connectionString(SB_CONN_STRING)
        .build();
  }
}
