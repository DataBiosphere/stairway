package bio.terra.stairway.impl;

import static bio.terra.stairway.FlightFilter.makeAnd;
import static bio.terra.stairway.FlightFilter.makeInputPredicate;
import static bio.terra.stairway.FlightFilter.makeOr;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightEnumeration;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightFilterSortDirection;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.fixtures.FlightsTestPojo;
import bio.terra.stairway.fixtures.TestStairwayBuilder;
import bio.terra.stairway.flights.TestFlightEnum1;
import bio.terra.stairway.flights.TestFlightEnum2;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
    Instant midSubmit = flights.get(2).getSubmitted();
    Instant maxSubmit = flights.get(5).getSubmitted();

    // -- Test Cases --

    // Case 1: date range
    FlightFilter filter =
        new FlightFilter()
            .addFilterSubmitTime(FlightFilterOp.GREATER_THAN, minSubmit)
            .addFilterSubmitTime(FlightFilterOp.LESS_THAN, maxSubmit);
    List<FlightState> flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 1", flightList, List.of("1", "2", "3", "4"));

    // Case 2: date range
    filter =
        new FlightFilter()
            .addFilterSubmitTime(FlightFilterOp.GREATER_EQUAL, minSubmit)
            .addFilterSubmitTime(FlightFilterOp.LESS_EQUAL, maxSubmit);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 2", flightList, List.of("0", "1", "2", "3", "4", "5"));

    // Case 3.1: date with values
    filter = new FlightFilter().addFilterCompletedTime(FlightFilterOp.GREATER_THAN, minSubmit);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 3.1", flightList, List.of("0", "1", "2"));

    // Case 3.2: date with null values
    filter = new FlightFilter().addFilterCompletedTime(FlightFilterOp.EQUAL, null);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 3.2", flightList, List.of("3", "4", "5"));

    // Case 4: status and flight class
    filter =
        new FlightFilter()
            .addFilterFlightStatus(FlightFilterOp.EQUAL, FlightStatus.RUNNING)
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlightEnum2.class);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 4", flightList, List.of("3", "4"));

    // Case 5: one in param
    filter = new FlightFilter().addFilterInputParameter("in0", FlightFilterOp.NOT_EQUAL, 5);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 5", flightList, List.of("2", "4"));

    // Case 6: class and one in param
    filter =
        new FlightFilter()
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlightEnum2.class)
            .addFilterInputParameter("in1", FlightFilterOp.EQUAL, "5");
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 6", flightList, List.of("1", "4"));

    // Case 7: pojo param
    filter = new FlightFilter().addFilterInputParameter("in2", FlightFilterOp.EQUAL, pojo2);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 7", flightList, List.of("2", "3"));

    // Case 8: three params
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

    // Case 10: page token
    String pageTokenString = new PageToken(midSubmit).makeToken();
    filter = new FlightFilter();
    FlightEnumeration flightEnum = flightDao.getFlights(null, 3, filter);
    checkResults("case 10", flightEnum.getFlightStateList(), List.of("0", "1", "2"));
    assertThat(flightEnum.getTotalFlights(), equalTo(6));
    assertThat(flightEnum.getNextPageToken(), equalTo(pageTokenString));

    flightEnum = flightDao.getFlights(pageTokenString, 3, filter);
    checkResults("case 10", flightEnum.getFlightStateList(), List.of("3", "4", "5"));
    assertThat(flightEnum.getTotalFlights(), equalTo(6));

    // Case 11: page token in descending order
    pageTokenString = new PageToken(flights.get(3).getSubmitted()).makeToken();
    filter = new FlightFilter().submittedTimeSortDirection(FlightFilterSortDirection.DESC);
    flightEnum = flightDao.getFlights(null, 3, filter);
    checkResults("case 11", flightEnum.getFlightStateList(), List.of("5", "4", "3"));
    assertThat(flightEnum.getTotalFlights(), equalTo(6));
    assertThat(flightEnum.getNextPageToken(), equalTo(pageTokenString));

    flightEnum = flightDao.getFlights(pageTokenString, 3, filter);
    checkResults("case 11", flightEnum.getFlightStateList(), List.of("2", "1", "0"));
    assertThat(flightEnum.getTotalFlights(), equalTo(6));

    // Case 12: sorting in ascending order
    filter = new FlightFilter().submittedTimeSortDirection(FlightFilterSortDirection.ASC);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 12", flightList, List.of("0", "1", "2", "3", "4", "5"));
    // explicitly verify that classnames are returned as expected (note that the flights list is in
    // ascending order)
    assertThat(
        "class names are correct",
        flightList.stream().map(FlightState::getClassName).collect(Collectors.toList()),
        contains(flights.stream().map(FlightState::getClassName).toArray()));

    // Case 13: sorting in descending order
    filter = new FlightFilter().submittedTimeSortDirection(FlightFilterSortDirection.DESC);
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 13", flightList, List.of("5", "4", "3", "2", "1", "0"));

    // Case 14: filter input on an in clause
    filter =
        new FlightFilter().addFilterInputParameter("in0", FlightFilterOp.IN, List.of(int1, int2));
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 14", flightList, List.of("1", "2", "3", "4"));

    // Case 15: filter flight on an in clause
    filter = new FlightFilter().addFilterFlightIds(List.of("0", "1", "3"));
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 15", flightList, List.of("0", "1", "3"));

    // Case 16: filter flight on a boolean clause (OR)...result should look similar to the in clause
    filter =
        new FlightFilter(
            makeOr(
                makeInputPredicate("in0", FlightFilterOp.EQUAL, int1),
                makeInputPredicate("in0", FlightFilterOp.EQUAL, int2)));
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 16", flightList, List.of("1", "2", "3", "4"));

    // Case 17: filter flight on a boolean clause (AND)
    filter =
        new FlightFilter(
            makeAnd(
                makeInputPredicate("in0", FlightFilterOp.EQUAL, int1),
                makeInputPredicate("in1", FlightFilterOp.EQUAL, string1)));
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 17", flightList, List.of("1"));

    // Case 18: filter flight with nested a boolean clauses
    filter =
        new FlightFilter(
            makeOr(
                makeAnd(
                    makeInputPredicate("in0", FlightFilterOp.EQUAL, int1),
                    makeInputPredicate("in1", FlightFilterOp.EQUAL, string1)),
                makeAnd(
                    makeInputPredicate("in0", FlightFilterOp.EQUAL, int2),
                    makeInputPredicate("in1", FlightFilterOp.EQUAL, string2))));

    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 18", flightList, List.of("1", "2"));

    // Case 19: filter flight with a boolean clause and an in clause
    filter =
        new FlightFilter(
            makeAnd(
                makeInputPredicate("in0", FlightFilterOp.EQUAL, int1),
                makeInputPredicate("in1", FlightFilterOp.IN, List.of(string1, string2))));

    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 19", flightList, List.of("1", "3"));

    // Case 20: filter flight with a null check for a field that doesn't exist
    filter = new FlightFilter().addFilterInputParameter("in10000", FlightFilterOp.EQUAL, null);

    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 20", flightList, List.of("0", "1", "2", "3", "4", "5"));

    // Case 21: filter input on an in clause with a POJO
    filter =
        new FlightFilter().addFilterInputParameter("in2", FlightFilterOp.IN, List.of(pojo1, pojo2));
    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 21", flightList, List.of("1", "2", "3", "4"));

    // Case 22: mix of generic boolean with input filter (the two get ANDed)
    filter =
        new FlightFilter(
                makeAnd(
                    makeInputPredicate("in0", FlightFilterOp.EQUAL, int1),
                    makeInputPredicate("in1", FlightFilterOp.IN, List.of(string1, string2))))
            .addFilterInputParameter("in2", FlightFilterOp.EQUAL, pojo1);

    flightList = flightDao.getFlights(0, 100, filter);
    checkResults("case 22", flightList, List.of("1"));
  }

  private void checkResults(String name, List<FlightState> resultlList, List<String> expectedIds) {
    List<String> actualIds =
        resultlList.stream().map(FlightState::getFlightId).collect(Collectors.toList());
    assertThat(
        name + ": elements match in the correct order", actualIds, contains(expectedIds.toArray()));
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

    Flight flight = FlightFactory.makeFlightFromName(className, inputParams, null);

    FlightContextImpl flightContext = new FlightContextImpl(stairway, flight, flightId, null);

    flightDao.create(flightContext);

    // If status isn't "RUNNING" then we set the status and mark the flight complete
    if (status != FlightStatus.RUNNING) {
      flightContext.setFlightStatus(status);
      flightDao.exit(flightContext);
    }

    return flightDao.getFlightState(flightId);
  }
}
