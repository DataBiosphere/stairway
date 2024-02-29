package bio.terra.stairway.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.stairway.Direction;
import bio.terra.stairway.fixtures.TestFlightContext;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.MDC;

@Tag("unit")
public class MdcHelperTest {
  private static final Map<String, String> FOO_BAR = Map.of("foo", "bar");
  private static final String FLIGHT_ID = "flightId" + UUID.randomUUID();
  private static final String FLIGHT_CLASS = "flightClass" + UUID.randomUUID();
  private static final Map<String, String> FLIGHT_MDC =
      Map.of(MdcHelper.FLIGHT_ID_KEY, FLIGHT_ID, MdcHelper.FLIGHT_CLASS_KEY, FLIGHT_CLASS);
  private static final int STEP_INDEX = 2;
  private static final Direction STEP_DIRECTION = Direction.DO;
  private static final String STEP_CLASS = "stepClass" + UUID.randomUUID();
  private static final Map<String, String> STEP_MDC =
      Map.of(
          MdcHelper.FLIGHT_STEP_NUMBER_KEY,
          Integer.toString(STEP_INDEX),
          MdcHelper.FLIGHT_STEP_DIRECTION_KEY,
          STEP_DIRECTION.toString(),
          MdcHelper.FLIGHT_STEP_CLASS_KEY,
          STEP_CLASS);

  private TestFlightContext flightContext;

  @BeforeEach
  void beforeEach() {
    MDC.clear();
    flightContext =
        new TestFlightContext()
            .flightId(FLIGHT_ID)
            .flightClassName(FLIGHT_CLASS)
            .stepIndex(STEP_INDEX)
            .direction(STEP_DIRECTION)
            .stepClassName(STEP_CLASS);
  }

  static Stream<Map<String, String>> initialContext() {
    return Stream.of(null, Map.of(), FOO_BAR);
  }

  @ParameterizedTest
  @MethodSource("initialContext")
  void withMdcAndFlightContext(Map<String, String> initialContext) {
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
    MdcHelper.withMdcAndFlightContext(runnable, flightContext).run();
  }

  @ParameterizedTest
  @MethodSource("initialContext")
  void withMdcAndFlightContext_subflight(Map<String, String> initialContext) {
    var expectedParentFlightMdc = new HashMap<>(FLIGHT_MDC);

    var childFlightId = "childFlightId";
    var childFlightClass = "childFlightClass";

    var childFlightContext =
        new TestFlightContext().flightId(childFlightId).flightClassName(childFlightClass);
    var expectedChildFlightMdc = new HashMap<>();
    expectedChildFlightMdc.put(MdcHelper.FLIGHT_ID_KEY, childFlightId);
    expectedChildFlightMdc.put(MdcHelper.FLIGHT_CLASS_KEY, childFlightClass);

    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedParentFlightMdc.putAll(initialContext);
      expectedChildFlightMdc.putAll(initialContext);
    }

    Runnable parentFlight =
        () -> {
          assertThat(
              "Calling thread's context with parent flight context",
              MDC.getCopyOfContextMap(),
              equalTo(expectedParentFlightMdc));
          // If a child flight is launched within a step in a parent flight, the calling thread's
          // context would also
          // contain step-specific context that should be cleared for logs emitted by the child
          // flight.
          MdcHelper.addStepContextToMdc(flightContext);
          Runnable childFlight =
              () ->
                  assertThat(
                      "Calling thread's context with child flight context",
                      MDC.getCopyOfContextMap(),
                      equalTo(expectedChildFlightMdc));
          MdcHelper.withMdcAndFlightContext(childFlight, childFlightContext).run();
        };

    MdcHelper.withMdcAndFlightContext(parentFlight, flightContext).run();
  }

  @ParameterizedTest
  @MethodSource("initialContext")
  void addAndRemoveStepContextFromMdc(Map<String, String> initialContext) {
    var expectedAddStepMdc = new HashMap<>();
    var expectedRemoveStepMdc = new HashMap<>();
    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedAddStepMdc.putAll(initialContext);
      expectedRemoveStepMdc.putAll(initialContext);
    }
    expectedAddStepMdc.putAll(STEP_MDC);

    MdcHelper.addStepContextToMdc(flightContext);
    assertThat(
        "Initial context with new step context",
        MDC.getCopyOfContextMap(),
        equalTo(expectedAddStepMdc));

    MdcHelper.removeStepContextFromMdc(flightContext);
    assertThat(
        "Initial context without step context",
        MDC.getCopyOfContextMap(),
        equalTo(expectedRemoveStepMdc));
  }
}
