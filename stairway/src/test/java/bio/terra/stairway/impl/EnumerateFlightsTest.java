package bio.terra.stairway.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.flights.TestFlightEnum1;
import bio.terra.stairway.flights.TestFlightEnum2;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class EnumerateFlightsTest {

  private StairwayImpl stairway;
  private FlightDao flightDao;

  @BeforeEach
  public void setup() throws Exception {
    stairway = (StairwayImpl) new TestStairwayBuilder().build();
    flightDao = stairway.getFlightDao();
  }

  @Test
  public void enumTest() throws Exception {
    FlightsTestPojo pojo1 = new FlightsTestPojo().anint(5).astring("5");
    FlightsTestPojo pojo2 = new FlightsTestPojo().anint(6).astring("6");
    int int1 = 5;
    int int2 = 6;
    String string1 = "5";
    String string2 = "6";
    String class1 = TestFlightEnum1.class.getName();
    String class2 = TestFlightEnum2.class.getName();

    // Build 6 flights with various parameters to allow testing of all ops on all datatypes
    // The input parameters get named "in1", "in2", "in3"
    List<FlightState> flights = new ArrayList<>();
    flights.add(makeFlight("0", FlightStatus.SUCCESS, class1, null));
    flights.add(makeFlight("1", FlightStatus.ERROR, class2, new Object[] {int1, string1, pojo1}));
    flights.add(makeFlight("2", FlightStatus.FATAL, class1, new Object[] {int2, string2, pojo2}));
    flights.add(makeFlight("3", FlightStatus.RUNNING, class2, new Object[] {int1, string2, pojo2}));
    flights.add(makeFlight("4", FlightStatus.RUNNING, class2, new Object[] {int2, string1, pojo1}));
    flights.add(makeFlight("5", FlightStatus.RUNNING, class1, null));

    Instant minSubmit = flights.get(0).getSubmitted();
    Instant maxSubmit = flights.get(5).getSubmitted();

    // -- Test Cases --

    // Case 1: date range - form1
    FlightFilter filter =
        new FlightFilter()
            .addFilterSubmitTime(FlightFilterOp.GREATER_THAN, minSubmit)
            .addFilterSubmitTime(FlightFilterOp.LESS_THAN, maxSubmit);
    List<FlightState> flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 1", flightList, Arrays.asList("1", "2", "3", "4"));

    // Case 2: date range - form1
    filter =
        new FlightFilter()
            .addFilterSubmitTime(FlightFilterOp.GREATER_EQUAL, minSubmit)
            .addFilterSubmitTime(FlightFilterOp.LESS_EQUAL, maxSubmit);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 2", flightList, Arrays.asList("0", "1", "2", "3", "4", "5"));

    // Case 3: date with null values - form1
    filter = new FlightFilter().addFilterCompletedTime(FlightFilterOp.GREATER_THAN, minSubmit);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 3", flightList, Arrays.asList("0", "1", "2"));

    // Case 4: status and flight class - form1
    filter =
        new FlightFilter()
            .addFilterFlightStatus(FlightFilterOp.EQUAL, FlightStatus.RUNNING)
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlightEnum2.class);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 4", flightList, Arrays.asList("3", "4"));

    // Case 5: one in param - form2
    filter = new FlightFilter().addFilterInputParameter("in0", FlightFilterOp.NOT_EQUAL, 5);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 5", flightList, Arrays.asList("2", "4"));

    // Case 6: class and one in param - form2
    filter =
        new FlightFilter()
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlightEnum2.class)
            .addFilterInputParameter("in1", FlightFilterOp.EQUAL, "5");
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 6", flightList, Arrays.asList("1", "4"));

    // Case 7: pojo param - form2
    filter = new FlightFilter().addFilterInputParameter("in2", FlightFilterOp.EQUAL, pojo2);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 7", flightList, Arrays.asList("2", "3"));

    // Case 8: three params - form3
    filter =
        new FlightFilter()
            .addFilterInputParameter("in0", FlightFilterOp.EQUAL, int2)
            .addFilterInputParameter("in1", FlightFilterOp.EQUAL, string1)
            .addFilterInputParameter("in2", FlightFilterOp.EQUAL, pojo1);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 8", flightList, Collections.singletonList("4"));

    // Case 9: submit data and two params
    filter =
        new FlightFilter()
            .addFilterSubmitTime(FlightFilterOp.GREATER_THAN, minSubmit)
            .addFilterInputParameter("in0", FlightFilterOp.EQUAL, int1)
            .addFilterInputParameter("in2", FlightFilterOp.EQUAL, pojo2);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 9", flightList, Collections.singletonList("3"));
  }

  private void checkResults(String name, List<FlightState> resultlList, List<String> expectedIds) {
    assertThat(
        name + ": right number of elements", resultlList.size(), equalTo(expectedIds.size()));
    List<String> actualIds =
        resultlList.stream().map(FlightState::getFlightId).collect(Collectors.toList());
    assertTrue(CollectionUtils.isEqualCollection(actualIds, expectedIds), "elements match");
  }

  private FlightState makeFlight(
      String flightId, FlightStatus status, String className, Object[] inputs) throws Exception {

    FlightMap inputParams = new FlightMap();
    int inputIndex = 0;
    if (inputs != null) {
      for (Object input : inputs) {
        inputParams.put("in" + inputIndex, input);
        inputIndex++;
      }
    }

    FlightFactory flightFactory = new FlightFactory();
    Flight flight = flightFactory.makeFlightFromName(className, inputParams, null);

    FlightContextImpl flightContext =
        new FlightContextImpl(
            stairway,
            flight,
            flightId,
            null);

    flightDao.create(flightContext);

    // If status isn't "RUNNING" then we set the status and mark the flight complete
    if (status != FlightStatus.RUNNING) {
      flightContext.setFlightStatus(status);
      flightDao.exit(flightContext);
    }

    return flightDao.getFlightState(flightId);
  }
}
