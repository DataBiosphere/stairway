package bio.terra.stairway.impl;

import static bio.terra.stairway.impl.ProgressMetersImpl.STAIRWAY_RESERVED_METER_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.ProgressMeter;
import bio.terra.stairway.ProgressMeterReader;
import bio.terra.stairway.exception.InvalidMeterName;
import bio.terra.stairway.fixtures.MapKey;
import bio.terra.stairway.fixtures.TestPauseController;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.flights.TestFlightProgress;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("unit")
public class ProgressTest {
  private final Logger logger = LoggerFactory.getLogger(ProgressTest.class);
  private StairwayImpl stairway;

  @BeforeEach
  public void setup() throws Exception {
    stairway = (StairwayImpl) new TestStairwayBuilder().build();
  }

  @Test
  public void progressValidMeterName() throws Exception {
    ProgressMetersImpl progressMeters =
        new ProgressMetersImpl(stairway.getFlightDao(), stairway.createFlightId());
    String badMeter = STAIRWAY_RESERVED_METER_PREFIX + "_bad_bad_bad";

    Assertions.assertThrows(
        InvalidMeterName.class, () -> progressMeters.setProgressMeter(badMeter, 3L, 5L));
  }

  @Test
  public void progressFlightTest() throws Exception {
    final long counterStart = 1;
    final long counterStop = 4;
    final String meter1 = "test-meter1";
    final String meter2 = "test-meter2";

    FlightMap inputs = new FlightMap();
    inputs.put(MapKey.COUNTER_START, counterStart);
    inputs.put(MapKey.COUNTER_STOP, counterStop);
    inputs.put(MapKey.PROGRESS_NAME1, meter1);
    inputs.put(MapKey.PROGRESS_NAME2, meter2);

    // Ensure the control is lower than the counterStart
    TestPauseController.setControl(0);

    String flightId = stairway.createFlightId();
    stairway.submit(flightId, TestFlightProgress.class, inputs);
    testMeter(flightId, meter1, counterStart, counterStop);
    testMeter(flightId, meter2, counterStart, counterStop);
  }

  private void testMeter(String flightId, String meterName, long counterStart, long counterStop)
      throws InterruptedException {
    for (long counter = counterStart; counter <= counterStop; counter++) {
      waitForProgress(flightId, meterName, counter, counterStop);
      logger.info("Set control to {}", counter);
      TestPauseController.setControl((int) counter);
      TimeUnit.SECONDS.sleep(1);
    }
    TestPauseController.setControl(0);
  }

  private void waitForProgress(
      String flightId, String meterName, long targetProgress, long targetMax)
      throws InterruptedException {
    // Three tries to get the status and then we barf
    for (int i = 0; i < 10; i++) {
      if (checkProgress(flightId, meterName, targetProgress, targetMax)) {
        logger.info("Meter {} progressed to {} of {}", meterName, targetProgress, targetMax);
        return;
      }
      logger.info(
          "Meter {} NOT progressed to {} of {} - wait {}", meterName, targetProgress, targetMax, i);
      TimeUnit.SECONDS.sleep(4);
    }
    logger.error("Meter named {} did not make progress in time", meterName);
    fail();
  }

  private boolean checkProgress(
      String flightId, String meterName, long targetProgress, long targetMax)
      throws InterruptedException {
    FlightState state = stairway.getFlightState(flightId);
    ProgressMeterReader progressMeters = state.getProgressMeters();

    ProgressMeter stepMeter = progressMeters.getFlightStepProgressMeter();
    logMeter("steps", stepMeter);

    ProgressMeter meterData = progressMeters.getProgressMeter(meterName);
    logMeter(meterName, meterData);
    if (meterData == null) {
      return false;
    }

    assertThat("meter stop is correct", meterData.getV2(), equalTo(targetMax));
    return (meterData.getV1() == targetProgress);
  }

  private void logMeter(String name, ProgressMeter meterData) {
    if (meterData == null) {
      logger.info("meter {} is not set", name);
    } else {
      logger.info("  meter {} at {} of {}", name, meterData.getV1(), meterData.getV2());
    }
  }
}
