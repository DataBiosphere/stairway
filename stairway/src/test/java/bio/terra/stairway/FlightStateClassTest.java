package bio.terra.stairway;

import static bio.terra.stairway.fixtures.TestUtil.dubValue;
import static bio.terra.stairway.fixtures.TestUtil.errString;
import static bio.terra.stairway.fixtures.TestUtil.fkey;
import static bio.terra.stairway.fixtures.TestUtil.flightId;
import static bio.terra.stairway.fixtures.TestUtil.ikey;
import static bio.terra.stairway.fixtures.TestUtil.intValue;
import static bio.terra.stairway.fixtures.TestUtil.skey;
import static bio.terra.stairway.fixtures.TestUtil.strValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("unit")
public class FlightStateClassTest {
  private static final String BAD = "bad bad bad";
  private static final String CLASS_NAME = "bio.terra.stairway.FooFlight";
  private FlightState result;
  private Instant timestamp;

  @BeforeEach
  public void setup() {
    FlightMap inputs = new FlightMap();
    inputs.put(ikey, intValue);
    inputs.put(skey, strValue);
    inputs.put(fkey, dubValue);

    FlightMap outputs = new FlightMap();
    outputs.put(ikey, intValue);
    outputs.put(skey, strValue);
    outputs.put(fkey, dubValue);

    timestamp = Instant.now();

    result = new FlightState();
    result.setFlightId(flightId);
    result.setFlightStatus(FlightStatus.FATAL);
    result.setInputParameters(inputs);
    result.setSubmitted(timestamp);
    result.setCompleted(timestamp);
    result.setResultMap(outputs);
    result.setException(new RuntimeException(errString));
    result.setClassName(CLASS_NAME);
  }

  @Test
  public void testFlightResultAccess() {
    assertThat(result.getFlightId(), is(flightId));
    assertThat(result.getFlightStatus(), is(FlightStatus.FATAL));
    assertThat(result.getInputParameters().get(skey, String.class), is(strValue));
    assertThat(result.getSubmitted(), is(timestamp));

    assertTrue(result.getCompleted().isPresent());
    assertTrue(result.getResultMap().isPresent());
    assertTrue(result.getException().isPresent());

    assertThat(result.getCompleted().get(), is(timestamp));
    assertThat(result.getException().get().getMessage(), is(errString));

    FlightMap outputMap = result.getResultMap().get();
    assertThat(outputMap.get(fkey, Double.class), is(dubValue));

    assertThat(result.getClassName(), is(CLASS_NAME));
  }

  @Test
  public void testResultMapIsImmutable() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          result.getResultMap().get().put(BAD, BAD);
        });
  }

  @Test
  public void testInputMapIsImmutable() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          result.getInputParameters().put(BAD, BAD);
        });
  }

  static Stream<Map<FlightStatus, Boolean>> activeStatuses() {
    return Stream.of(
        Map.of(FlightStatus.RUNNING, true),
        Map.of(FlightStatus.SUCCESS, false),
        Map.of(FlightStatus.ERROR, false),
        Map.of(FlightStatus.FATAL, false),
        Map.of(FlightStatus.WAITING, true),
        Map.of(FlightStatus.READY, true),
        Map.of(FlightStatus.QUEUED, true),
        Map.of(FlightStatus.READY_TO_RESTART, true));
  }

  @ParameterizedTest
  @MethodSource("activeStatuses")
  void testActiveFlightStatuses(Map<FlightStatus, Boolean> activeStates) {
    for (Map.Entry<FlightStatus, Boolean> entry : activeStates.entrySet()) {
      FlightState flightState = new FlightState();
      flightState.setFlightStatus(entry.getKey());
      boolean isActive = entry.getValue();
      assertThat(flightState.isActive(), is(isActive));
    }
  }
}
