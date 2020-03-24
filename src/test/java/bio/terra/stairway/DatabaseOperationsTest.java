package bio.terra.stairway;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static bio.terra.stairway.TestUtil.dubValue;
import static bio.terra.stairway.TestUtil.errString;
import static bio.terra.stairway.TestUtil.fkey;
import static bio.terra.stairway.TestUtil.flightId;
import static bio.terra.stairway.TestUtil.ikey;
import static bio.terra.stairway.TestUtil.intValue;
import static bio.terra.stairway.TestUtil.skey;
import static bio.terra.stairway.TestUtil.strValue;
import static bio.terra.stairway.TestUtil.wfkey;
import static bio.terra.stairway.TestUtil.wikey;
import static bio.terra.stairway.TestUtil.wskey;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class DatabaseOperationsTest {
    @Test
    public void basicsTest() throws Exception {
        Stairway stairway = TestUtil.setupDummyStairway();
        String stairwayId = stairway.getStairwayId();
        FlightDao flightDao = stairway.getFlightDao();

        FlightMap inputs = new FlightMap();
        inputs.put(ikey, intValue);
        inputs.put(skey, strValue);
        inputs.put(fkey, dubValue);

        FlightContext flightContext = new FlightContext(inputs, "notArealClass");
        flightContext.setFlightId(flightId);
        flightContext.setStairway(stairway);

        flightDao.submit(flightContext);

        // Use recover to retrieve the internal state of the flight
        List<FlightContext> flightList = flightDao.recover(stairwayId);
        assertThat(flightList.size(), is(equalTo(1)));
        FlightContext recoveredFlight = flightList.get(0);

        assertThat(recoveredFlight.getFlightId(), is(equalTo(flightContext.getFlightId())));
        assertThat(recoveredFlight.getFlightClassName(), is(equalTo(flightContext.getFlightClassName())));
        assertThat(recoveredFlight.getStepIndex(), is(equalTo(0)));
        assertThat(recoveredFlight.isDoing(), is(true));
        assertThat(recoveredFlight.getResult().isSuccess(), is(true));
        assertThat(recoveredFlight.getFlightStatus(), CoreMatchers.is(FlightStatus.RUNNING));

        FlightMap recoveredInputs = recoveredFlight.getInputParameters();
        checkInputs(recoveredInputs);

        // Use getFlightState to retrieve the externally visible state of the flight
        FlightState flightState = flightDao.getFlightState(flightId);
        checkRunningFlightState(flightState);
        FlightMap stateInputs = flightState.getInputParameters();
        checkInputs(stateInputs);

        flightContext.setStepIndex(1);
        flightDao.step(flightContext);

        Thread.sleep(1000);

        flightContext.setDoing(false);
        flightContext.setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(errString)));
        flightContext.setStepIndex(2);
        flightContext.setDoing(false);
        flightContext.getWorkingMap().put(wfkey, dubValue);
        flightContext.getWorkingMap().put(wikey, intValue);
        flightContext.getWorkingMap().put(wskey, strValue);

        flightDao.step(flightContext);

        flightList = flightDao.recover(stairwayId);
        assertThat(flightList.size(), is(equalTo(1)));
        recoveredFlight = flightList.get(0);
        assertThat(recoveredFlight.getStepIndex(), is(equalTo(2)));
        assertThat(recoveredFlight.isDoing(), is(false));
        assertThat(recoveredFlight.getResult().isSuccess(), is(false));
        assertThat(recoveredFlight.getResult().getException().get().toString(), containsString(errString));
        assertThat(recoveredFlight.getFlightStatus(), is(FlightStatus.RUNNING));

        FlightMap recoveredWork = recoveredFlight.getWorkingMap();
        checkOutputs(recoveredWork);

        flightState = flightDao.getFlightState(flightId);
        checkRunningFlightState(flightState);
        stateInputs = flightState.getInputParameters();
        checkInputs(stateInputs);

        flightContext.setFlightStatus(FlightStatus.ERROR);

        flightDao.exit(flightContext);

        flightList = flightDao.recover(stairwayId);
        assertThat(flightList.size(), is(equalTo(0)));

        List<FlightState> flightStateList = flightDao.getFlights(0, 99, null);
        assertThat(flightStateList.size(), is(1));
        flightState = flightStateList.get(0);
        assertThat(flightState.getFlightId(), is(flightId));
        assertThat(flightState.getFlightStatus(), is(FlightStatus.ERROR));
        assertTrue(flightState.getResultMap().isPresent());
        assertTrue(flightState.getException().isPresent());

        FlightMap outputParams = flightState.getResultMap().get();
        checkOutputs(outputParams);
        assertThat(flightState.getException().get().toString(), containsString(errString));
    }

    private void checkRunningFlightState(FlightState flightState) {
        assertThat(flightState.getFlightId(), is(flightId));
        assertThat(flightState.getFlightStatus(), is(FlightStatus.RUNNING));
        assertFalse(flightState.getCompleted().isPresent());
        assertFalse(flightState.getResultMap().isPresent());
        assertFalse(flightState.getException().isPresent());
    }

    private void checkInputs(FlightMap inputMap) {
        assertThat(inputMap.get(fkey, Double.class), is(equalTo(dubValue)));
        assertThat(inputMap.get(skey, String.class), is(equalTo(strValue)));
        assertThat(inputMap.get(ikey, Integer.class), is(equalTo(intValue)));
    }

    private void checkOutputs(FlightMap outputMap) {
        assertThat(outputMap.get(wfkey, Double.class), is(equalTo(dubValue)));
        assertThat(outputMap.get(wskey, String.class), is(equalTo(strValue)));
        assertThat(outputMap.get(wikey, Integer.class), is(equalTo(intValue)));
    }
}
