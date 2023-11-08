package bio.terra.stairway.azure;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(
    value = "THROWS_METHOD_THROWS_CLAUSE_THROWABLE",
    justification = "Includes test-only lambdas that throw exceptions")
class AzureServiceBusQueueTest {

  private AzureServiceBusQueue azureServiceBusQueue;
  @Mock private ServiceBusReceivedMessage receivedMessage;
  @Mock private ServiceBusReceiverClient serviceBusReceiverClient;
  @Mock private ServiceBusSenderClient serviceBusSenderClient;

  @BeforeEach
  void setUp() {
    azureServiceBusQueue =
        new AzureServiceBusQueue(serviceBusReceiverClient, serviceBusSenderClient);
  }

  @Test
  void dispatchMessages_messageIsReceivedAndProcessedSuccessfully_messageIsComplete()
      throws InterruptedException {

    // set up service bus receiver client mock to return a message
    setUpReceiverClient();

    azureServiceBusQueue.dispatchMessages(1, message -> true);

    // verify that the message is set complete
    verify(serviceBusReceiverClient, times(1)).complete(receivedMessage);
  }

  @Test
  void dispatchMessages_messageIsReceivedAndProcessedUnSuccessfully_messageIsAbandon()
      throws InterruptedException {

    // set up service bus receiver client mock to return a message
    setUpReceiverClient();

    azureServiceBusQueue.dispatchMessages(1, message -> false);

    // verify that the message is set complete
    verify(serviceBusReceiverClient, times(0)).complete(receivedMessage);
    verify(serviceBusReceiverClient, times(1)).abandon(receivedMessage);
  }

  @Test
  void dispatchMessages_messageIsReceivedProcessingThrows_MessageIsAbandon()
      throws InterruptedException {

    // set up service bus receiver client mock to return a message
    setUpReceiverClient();

    azureServiceBusQueue.dispatchMessages(
        1,
        message -> {
          throw new ServiceBusException(new RuntimeException("test"), new ServiceBusErrorSource());
        });

    // verify that the message is set complete
    verify(serviceBusReceiverClient, times(0)).complete(receivedMessage);
    verify(serviceBusReceiverClient, times(1)).abandon(receivedMessage);
  }

  @Test
  void dispatchMessages_interruptedExceptionIsThrown_throwsInterruptedException() {

    // set up service bus receiver client mock to return a message
    setUpReceiverClient();

    assertThrows(
        InterruptedException.class,
        () ->
            azureServiceBusQueue.dispatchMessages(
                1,
                message -> {
                  throw new InterruptedException("test");
                }));
  }

  private void setUpReceiverClient() {
    when(serviceBusReceiverClient.receiveMessages(1))
        .thenReturn(IterableStream.of(List.of(receivedMessage)));
    when(receivedMessage.getBody()).thenReturn(BinaryData.fromString("test"));
  }

  @Test
  void enqueueMessage_messageEnqueued_clientSendsMessage() {

    azureServiceBusQueue.enqueueMessage("test");

    verify(serviceBusSenderClient, times(1)).sendMessage(any());
  }
}
