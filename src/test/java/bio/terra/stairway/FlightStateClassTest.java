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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class FlightStateClassTest {
  private static String bad = "bad bad bad";
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
  }

  @Test
  public void testResultMapIsImmutable() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          result.getResultMap().get().put(bad, bad);
        });
  }

  @Test
  public void testInputMapIsImmutable() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          result.getInputParameters().put(bad, bad);
        });
  }
}
