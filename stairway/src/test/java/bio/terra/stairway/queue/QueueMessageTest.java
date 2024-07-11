package bio.terra.stairway.queue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.impl.MdcUtils;
import bio.terra.stairway.impl.StairwayImpl;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.MDC;

@Tag("unit")
@ExtendWith(MockitoExtension.class)
public class QueueMessageTest {

  @Mock private StairwayImpl stairway;
  private static final String FLIGHT_ID = "flight-abc";
  private static final Map<String, String> CALLING_THREAD_CONTEXT =
      Map.of("requestId", "request-abc");

  @BeforeEach
  void beforeEach() {
    MDC.clear();
  }

  private static Stream<Map<String, String>> message_serde() {
    return Stream.of(null, CALLING_THREAD_CONTEXT);
  }

  @ParameterizedTest
  @MethodSource
  public void message_serde(Map<String, String> expectedMdc) {
    MdcUtils.overwriteContext(expectedMdc);
    QueueMessageReady messageReady = new QueueMessageReady(FLIGHT_ID);
    WorkQueueProcessor workQueueProcessor = new WorkQueueProcessor(stairway);

    // Now we add something else to the MDC, but it won't show up in our deserialized queue message.
    MDC.put("another-key", "another-value");

    String serialized = workQueueProcessor.serialize(messageReady);
    QueueMessage deserialized = workQueueProcessor.deserialize(serialized);
    assertThat(deserialized, instanceOf(QueueMessageReady.class));

    QueueMessageReady messageReadyCopy = (QueueMessageReady) deserialized;
    assertThat(messageReadyCopy.getFlightId(), equalTo(messageReady.getFlightId()));
    assertThat(
        messageReadyCopy.getType().getMessageEnum(),
        equalTo(messageReady.getType().getMessageEnum()));
    assertThat(
        messageReadyCopy.getType().getVersion(), equalTo(messageReady.getType().getVersion()));
    assertThat(messageReadyCopy.getFlightId(), equalTo(messageReady.getFlightId()));
    assertThat(messageReadyCopy.getCallingThreadContext(), equalTo(expectedMdc));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void process(boolean resumeAnswer) throws InterruptedException {
    QueueMessageReady messageReady = new QueueMessageReady(FLIGHT_ID);
    messageReady.setCallingThreadContext(CALLING_THREAD_CONTEXT);

    when(stairway.resume(FLIGHT_ID))
        .thenAnswer(
            (Answer<Boolean>)
                invocation -> {
                  assertThat(
                      "MDC is set during processing",
                      MDC.getCopyOfContextMap(),
                      equalTo(CALLING_THREAD_CONTEXT));
                  return resumeAnswer;
                });

    assertThat(
        "Message is considered processed when stairway.resume returns " + resumeAnswer,
        messageReady.process(stairway),
        equalTo(true));
    assertThat("MDC is reverted after processing", MDC.getCopyOfContextMap(), equalTo(null));
  }

  @Test
  public void process_DatabaseOperationException() throws InterruptedException {
    QueueMessageReady messageReady = new QueueMessageReady(FLIGHT_ID);
    messageReady.setCallingThreadContext(CALLING_THREAD_CONTEXT);

    doThrow(DatabaseOperationException.class).when(stairway).resume(FLIGHT_ID);

    assertThat(
        "Message is left on the queue when stairway.resume throws",
        messageReady.process(stairway),
        equalTo(false));
    assertThat(MDC.getCopyOfContextMap(), equalTo(null));
  }
}
