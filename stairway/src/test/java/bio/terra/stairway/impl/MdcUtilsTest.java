package bio.terra.stairway.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.stairway.Direction;
import bio.terra.stairway.exception.StairwayExecutionException;
import bio.terra.stairway.fixtures.TestFlightContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.MDC;

@Tag("unit")
class MdcUtilsTest {
  private static final Map<String, String> INITIAL_CONTEXT = Map.of("initial", "context");
  private static final Map<String, String> FOO_BAR = Map.of("foo", "bar");
  private static final String FLIGHT_ID = "flightId" + UUID.randomUUID();
  private static final String FLIGHT_CLASS = "flightClass" + UUID.randomUUID();
  private static final Map<String, String> FLIGHT_MDC =
      Map.of(MdcUtils.FLIGHT_ID_KEY, FLIGHT_ID, MdcUtils.FLIGHT_CLASS_KEY, FLIGHT_CLASS);
  private static final int STEP_INDEX = 2;
  private static final Direction STEP_DIRECTION = Direction.DO;
  private static final String STEP_CLASS = "stepClass" + UUID.randomUUID();
  private static final Map<String, String> STEP_MDC =
      Map.of(
          MdcUtils.FLIGHT_STEP_NUMBER_KEY,
          Integer.toString(STEP_INDEX),
          MdcUtils.FLIGHT_STEP_DIRECTION_KEY,
          STEP_DIRECTION.toString(),
          MdcUtils.FLIGHT_STEP_CLASS_KEY,
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

  static Stream<Map<String, String>> contextMap() {
    return Stream.of(null, Map.of(), FOO_BAR);
  }

  @ParameterizedTest
  @MethodSource("contextMap")
  void callWithContext(Map<String, String> newContext) throws InterruptedException {
    MDC.setContextMap(INITIAL_CONTEXT);
    Boolean result =
        MdcUtils.callWithContext(
            newContext,
            () -> {
              assertThat(
                  "Context is overwritten during computation",
                  MDC.getCopyOfContextMap(),
                  equalTo(newContext));
              return true;
            });
    assertThat("Result of computation is returned", result, equalTo(true));
    assertThat("Initial context is restored", MDC.getCopyOfContextMap(), equalTo(INITIAL_CONTEXT));
  }

  static Stream<Arguments> callWithContext_exception() {
    List<Arguments> arguments = new ArrayList<>();
    for (var newContext : contextMap().toList()) {
      arguments.add(
          Arguments.of(
              newContext, new InterruptedException("interrupted"), InterruptedException.class));
      arguments.add(
          Arguments.of(
              newContext, new RuntimeException("unexpected"), StairwayExecutionException.class));
    }
    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource
  <T extends Throwable> void callWithContext_exception(
      Map<String, String> newContext, Exception exception, Class<T> expectedExceptionClass) {
    MDC.setContextMap(INITIAL_CONTEXT);
    assertThrows(
        expectedExceptionClass,
        () ->
            MdcUtils.callWithContext(
                newContext,
                () -> {
                  assertThat(
                      "Context is overwritten during computation",
                      MDC.getCopyOfContextMap(),
                      equalTo(newContext));
                  throw exception;
                }));
    assertThat("Initial context is restored", MDC.getCopyOfContextMap(), equalTo(INITIAL_CONTEXT));
  }

  @ParameterizedTest
  @MethodSource("contextMap")
  void overwriteContext(Map<String, String> newContext) {
    MDC.setContextMap(FOO_BAR);
    MdcUtils.overwriteContext(newContext);
    assertThat("MDC overwritten by new context", MDC.getCopyOfContextMap(), equalTo(newContext));
  }

  @ParameterizedTest
  @MethodSource("contextMap")
  void addFlightContextToMdc(Map<String, String> initialContext) {
    var expectedMdc = new HashMap<>();
    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedMdc.putAll(initialContext);
    }
    expectedMdc.putAll(FLIGHT_MDC);

    MdcUtils.addFlightContextToMdc(flightContext);
    assertThat(
        "Initial context with flight context", MDC.getCopyOfContextMap(), equalTo(expectedMdc));
  }

  @ParameterizedTest
  @MethodSource("contextMap")
  void addAndRemoveStepContextFromMdc(Map<String, String> initialContext) {
    var expectedAddStepMdc = new HashMap<>();
    var expectedRemoveStepMdc = new HashMap<>();
    if (initialContext != null) {
      MDC.setContextMap(initialContext);
      expectedAddStepMdc.putAll(initialContext);
      expectedRemoveStepMdc.putAll(initialContext);
    }
    expectedAddStepMdc.putAll(STEP_MDC);

    MdcUtils.addStepContextToMdc(flightContext);
    assertThat(
        "Initial context with new step context",
        MDC.getCopyOfContextMap(),
        equalTo(expectedAddStepMdc));

    MdcUtils.removeStepContextFromMdc(flightContext);
    assertThat(
        "Initial context without step context",
        MDC.getCopyOfContextMap(),
        equalTo(expectedRemoveStepMdc));
  }
}
