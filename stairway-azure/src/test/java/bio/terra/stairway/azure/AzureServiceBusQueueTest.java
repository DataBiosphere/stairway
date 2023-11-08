package bio.terra.stairway.azure;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;



import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
class AzureServiceBusQueueTest {

    private AzureServiceBusQueue azureServiceBusQueue;
    @Mock
    private ServiceBusReceivedMessage receivedMessage;
    @Mock
    private ServiceBusReceiverClient serviceBusReceiverClient;
    @Mock
    private ServiceBusSenderClient serviceBusSenderClient;

    @BeforeEach
    void setUp() {
        azureServiceBusQueue = new AzureServiceBusQueue(serviceBusReceiverClient, serviceBusSenderClient);
    }

    @Test
    void newBuilder() {
    }

    @Test
    void dispatchMessages_messageIsReceivedAndProcessedSuccessfully_messageIsComplete() throws InterruptedException {

        //set up service bus receiver client mock to return a message
        when(serviceBusReceiverClient.receiveMessages(1))
                .thenReturn(IterableStream.of(List.of(receivedMessage)));
        when(receivedMessage.getBody()).thenReturn(BinaryData.fromString("test"));

        azureServiceBusQueue.dispatchMessages(1, message -> true);

        //verify that the message is set complete
        verify(serviceBusReceiverClient,times(1)).complete(receivedMessage);
    }
    @Test
    void dispatchMessages_messageIsReceivedAndProcessedUnSuccessfully_messageIsAbandon() throws InterruptedException {

        //set up service bus receiver client mock to return a message
        when(serviceBusReceiverClient.receiveMessages(1))
                .thenReturn(IterableStream.of(List.of(receivedMessage)));
        when(receivedMessage.getBody()).thenReturn(BinaryData.fromString("test"));

        azureServiceBusQueue.dispatchMessages(1, message -> false);

        //verify that the message is set complete
        verify(serviceBusReceiverClient,times(0)).complete(receivedMessage);
        verify(serviceBusReceiverClient,times(1)).abandon(receivedMessage);
    }
    @Test
    void dispatchMessages_messageIsReceivedProcessingThrows_MessageIsAbandon() throws InterruptedException {

        //set up service bus receiver client mock to return a message
        when(serviceBusReceiverClient.receiveMessages(1))
                .thenReturn(IterableStream.of(List.of(receivedMessage)));
        when(receivedMessage.getBody()).thenReturn(BinaryData.fromString("test"));

        azureServiceBusQueue.dispatchMessages(1, message -> {throw new RuntimeException("test");});

        //verify that the message is set complete
        verify(serviceBusReceiverClient,times(0)).complete(receivedMessage);
        verify(serviceBusReceiverClient,times(1)).abandon(receivedMessage);
    }

    @Test
    void enqueueMessage() {
    }
}