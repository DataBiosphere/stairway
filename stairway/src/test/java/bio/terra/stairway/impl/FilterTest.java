package bio.terra.stairway.impl;

import static java.time.Instant.now;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilter.FlightFilterPredicate;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightFilterSortDirection;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.flights.TestFlight;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class FilterTest {

  @Test
  public void predicateTest() throws Exception {
    FlightFilter filter = new FlightFilter();
    filter.addFilterInputParameter("afield", FlightFilterOp.NOT_EQUAL, "avalue");
    filter.addFilterInputParameter("afield", FlightFilterOp.GREATER_EQUAL, "avalue");
    filter.addFilterInputParameter("afield", FlightFilterOp.GREATER_THAN, "avalue");
    filter.addFilterInputParameter("afield", FlightFilterOp.LESS_EQUAL, "avalue");
    filter.addFilterInputParameter("afield", FlightFilterOp.LESS_THAN, "avalue");

    testPredicate(filter, 0, "!=");
    testPredicate(filter, 1, ">=");
    testPredicate(filter, 2, ">");
    testPredicate(filter, 3, "<=");
    testPredicate(filter, 4, "<");
  }

  private void testPredicate(FlightFilter filter, int index, String operand) {
    String flightCompareSql = String.format("F.afield %s :ff%d", operand, index + 1);
    String inputCompareSql =
        String.format("(I.key = 'afield' AND I.value %s :ff%d)", operand, index + 1);

    FlightFilterAccess access = new FlightFilterAccess(filter, 0, 10, null);

    FlightFilterPredicate predicate = filter.getInputPredicates().get(index);
    String flightSql = access.makeFlightPredicateSql(predicate);
    assertThat(flightSql, equalTo(flightCompareSql));
    String inputSql = access.makeInputPredicateSql(predicate);
    assertThat(inputSql, equalTo(inputCompareSql));
  }

  // There next 6 tests are all the possibilities for
  // query forms 1, 2, and 3 with and without flight filters
  @Test
  public void filterForm1NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    FlightFilter filter = new FlightFilter();
    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F WHERE"
            + " F.completed_time > :ff1 AND F.class_name = :ff2 AND F.status = :ff3 AND F.submit_time < :ff4"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    Instant submit = now();
    Instant complete = now();
    FlightFilter filter =
        new FlightFilter()
            .addFilterCompletedTime(FlightFilterOp.GREATER_THAN, submit)
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class)
            .addFilterFlightStatus(FlightFilterOp.EQUAL, FlightStatus.RUNNING)
            .addFilterSubmitTime(FlightFilterOp.LESS_THAN, complete);
    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm2NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F INNER JOIN flightinput I"
            + " ON F.flightid = I.flightid"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com");

    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm2WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F INNER JOIN flightinput I"
            + " ON F.flightid = I.flightid"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " AND F.class_name = :ff2"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class);

    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm3NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F INNER JOIN "
            + "(SELECT flightid, COUNT(*) AS matchCount FROM flightinput I"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " OR (I.key = 'name' AND I.value = :ff2)"
            + " GROUP BY I.flightid) INPUT"
            + " ON F.flightid = INPUT.flightid"
            + " WHERE INPUT.matchCount = 2"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterInputParameter("name", FlightFilterOp.EQUAL, "dd");

    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm3WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F INNER JOIN "
            + "(SELECT flightid, COUNT(*) AS matchCount FROM flightinput I"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " OR (I.key = 'name' AND I.value = :ff2)"
            + " GROUP BY I.flightid) INPUT"
            + " ON F.flightid = INPUT.flightid"
            + " WHERE INPUT.matchCount = 2"
            + " AND F.class_name = :ff3"
            + " ORDER BY submit_time ASC LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterInputParameter("name", FlightFilterOp.EQUAL, "dd")
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class);

    String sql = new FlightFilterAccess(filter, 0, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1NoLimitTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F"
            + " ORDER BY submit_time ASC OFFSET :offset";

    FlightFilter filter = new FlightFilter();
    String sql = new FlightFilterAccess(filter, 0, null, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1NoOffsetTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F"
            + " ORDER BY submit_time ASC LIMIT :limit";

    FlightFilter filter = new FlightFilter();
    String sql = new FlightFilterAccess(filter, null, 10, null).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1PageTokenTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F"
            + " WHERE F.submit_time > :pagetoken"
            + " ORDER BY submit_time ASC LIMIT :limit";

    PageToken pageToken = new PageToken(Instant.now());

    FlightFilter filter = new FlightFilter();
    String sql = new FlightFilterAccess(filter, null, 10, pageToken.makeToken()).makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1OrderTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.status, F.serialized_exception, F.class_name"
            + " FROM flight F"
            + " WHERE F.submit_time < :pagetoken"
            + " ORDER BY submit_time DESC LIMIT :limit";

    PageToken pageToken = new PageToken(Instant.now());

    FlightFilter filter =
        new FlightFilter().submittedTimeSortDirection(FlightFilterSortDirection.DESC);
    String sql = new FlightFilterAccess(filter, null, 10, pageToken.makeToken()).makeSql();
    assertThat(sql, equalTo(expect));
  }
}
