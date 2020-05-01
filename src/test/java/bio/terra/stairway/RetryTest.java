package bio.terra.stairway;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class RetryTest {
    private Stairway stairway;

    @BeforeEach
    public void setup() throws Exception {
        stairway = TestUtil.setupDefaultStairway();
    }

    @Test
    public void fixedSuccessTest() throws Exception {
        // Fixed interval where maxCount > failCount should succeed
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", 2);
        inputParameters.put("intervalSeconds", 2);
        inputParameters.put("maxCount", 4);

        String flightId = "successTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters);
        FlightState result = stairway.waitForFlight(flightId, null, null);
        assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        assertFalse(result.getException().isPresent());
    }

    @Test
    public void fixedFailureTest() throws Exception {
        // Fixed interval where maxCount =< failCount should fail
        int intervalSeconds = 2;
        int maxCount = 3;

        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", 100);
        inputParameters.put("intervalSeconds", intervalSeconds);
        inputParameters.put("maxCount", maxCount);

        // fail time should be >= maxCount * intervalSeconds
        // and not too long... whatever that is. How about (maxCount+1 * intervalSeconds
        LocalDateTime startTime = LocalDateTime.now();
        String flightId = "failureTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters);
        FlightState result = stairway.waitForFlight(flightId, 1, null);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(maxCount * intervalSeconds));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds((maxCount + 1) * intervalSeconds));
        assertTrue(endTime.isAfter(startRange));
        assertTrue(endTime.isBefore(endRange));
        assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));
    }

    @Test
    public void exponentialSuccessTest() throws Exception {
        // Exponential with generous limits
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", 2);
        inputParameters.put("initialIntervalSeconds", 1L);
        inputParameters.put("maxIntervalSeconds", 100L);
        inputParameters.put("maxOperationTimeSeconds", 100L);

        String flightId = "exponentialTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters);
        FlightState result = stairway.waitForFlight(flightId, null, null);
        assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        assertFalse(result.getException().isPresent());
    }

    @Test
    public void exponentialOpTimeFailureTest() throws Exception {
        // Should fail by running out of operation time
        // Should go 2 + 4 + 8 + 16 - well over 10
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", 4);
        inputParameters.put("initialIntervalSeconds", 2L);
        inputParameters.put("maxIntervalSeconds", 100L);
        inputParameters.put("maxOperationTimeSeconds", 10L);

        String flightId = "expOpTimeTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters);

        FlightState result = stairway.waitForFlight(flightId, null, null);
        assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
        assertTrue(result.getException().isPresent());
    }

    @Test
    public void exponentialMaxIntervalSuccessTest() throws Exception {
        // Should succeed in 4 tries. The time should be capped by
        // the maxInterval of 4. That is,
        // 2 + 4 + 4 + 4 = 14 should be less than 2 + 4 + 8 + 16 = 30
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", 4);
        inputParameters.put("initialIntervalSeconds", 2L);
        inputParameters.put("maxIntervalSeconds", 4L);
        inputParameters.put("maxOperationTimeSeconds", 100L);

        LocalDateTime startTime = LocalDateTime.now();
        String flightId = "expMaxTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters);
        FlightState result = stairway.waitForFlight(flightId, null, null);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(14));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds(30));
        assertTrue(endTime.isAfter(startRange));
        assertTrue(endTime.isBefore(endRange));

        assertThat(result.getFlightStatus(), is(FlightStatus.SUCCESS));
        assertFalse(result.getException().isPresent());
    }

}
