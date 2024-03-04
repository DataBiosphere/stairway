package bio.terra.stairway.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.stairway.fixtures.TestFlightContext;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.MDC;

@Tag("unit")
class StairwayThreadPoolTest {

  private static final int MAX_PARALLEL_FLIGHTS = 5;
  private static final Map<String, String> FOO_BAR = Map.of("foo", "bar");
  private static final String FLIGHT_ID = "flightId" + UUID.randomUUID();
  private static final String FLIGHT_CLASS = "flightClass" + UUID.randomUUID();
  private static final Map<String, String> FLIGHT_MDC =
      Map.of(MdcUtils.FLIGHT_ID_KEY, FLIGHT_ID, MdcUtils.FLIGHT_CLASS_KEY, FLIGHT_CLASS);

  private StairwayThreadPool stairwayThreadPool;
  private TestFlightContext flightContext;

  @BeforeEach
  void beforeEach() {
    MDC.clear();
    stairwayThreadPool = new StairwayThreadPool(MAX_PARALLEL_FLIGHTS);
    flightContext = new TestFlightContext().flightId(FLIGHT_ID).flightClassName(FLIGHT_CLASS);
  }

  static Stream<Map<String, String>> initialContext() {
    return Stream.of(null, Map.of(), FOO_BAR);
  }

  @ParameterizedTest
  @MethodSource("initialContext")
  void submitWithMdcAndFlightContext(Map<String, String> initialContext)
      throws ExecutionException, InterruptedException, TimeoutException {
    var expectedMdc = new HashMap<>(FLIGHT_MDC);
    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedMdc.putAll(initialContext);
    }

    Runnable runnable =
        () ->
            assertThat(
                "Calling thread's context with new flight context",
                MDC.getCopyOfContextMap(),
                equalTo(expectedMdc));

    stairwayThreadPool
        .submitWithMdcAndFlightContext(runnable, flightContext)
        .get(1, TimeUnit.SECONDS);

    assertThat(
        "Calling thread's context is unchanged",
        MDC.getCopyOfContextMap(),
        equalTo(initialContext));
  }

  @ParameterizedTest
  @MethodSource("initialContext")
  void submitWithMdcAndFlightContext_subflight(Map<String, String> initialContext)
      throws ExecutionException, InterruptedException {
    var expectedParentFlightMdc = new HashMap<>(FLIGHT_MDC);

    var childFlightId = "childFlightId";
    var childFlightClass = "childFlightClass";

    var childFlightContext =
        new TestFlightContext().flightId(childFlightId).flightClassName(childFlightClass);
    var expectedChildFlightMdc = new HashMap<>();
    expectedChildFlightMdc.put(MdcUtils.FLIGHT_ID_KEY, childFlightId);
    expectedChildFlightMdc.put(MdcUtils.FLIGHT_CLASS_KEY, childFlightClass);

    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedParentFlightMdc.putAll(initialContext);
      expectedChildFlightMdc.putAll(initialContext);
    }

    Runnable childFlight =
        () ->
            assertThat(
                "Calling thread's context with child flight context",
                MDC.getCopyOfContextMap(),
                equalTo(expectedChildFlightMdc));
    Runnable parentFlight =
        () -> {
          assertThat(
              "Calling thread's context with parent flight context",
              MDC.getCopyOfContextMap(),
              equalTo(expectedParentFlightMdc));
          // If a child flight is launched within a step in a parent flight, the calling thread's
          // context would also contain step-specific context that should be cleared for logs
          // emitted by the child flight.
          MdcUtils.addStepContextToMdc(flightContext);
          try {
            stairwayThreadPool
                .submitWithMdcAndFlightContext(childFlight, childFlightContext)
                .get(1, TimeUnit.SECONDS);
          } catch (Exception e) {
            fail("Unexpected exception waiting for child flight", e);
          }
        };
    stairwayThreadPool.submitWithMdcAndFlightContext(parentFlight, flightContext).get();

    assertThat(
        "Calling thread's context is unchanged",
        MDC.getCopyOfContextMap(),
        equalTo(initialContext));
  }
}
