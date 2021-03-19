package bio.terra.stairway;

import static java.time.Instant.now;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.stairway.flights.TestFlight;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class FilterTest {

  @Test
  public void predicateTest() throws Exception {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            "afield",
            FlightFilterOp.NOT_EQUAL,
            "avalue",
            FlightFilterPredicate.Datatype.STRING,
            "p1");

    String flightSql = predicate.makeFlightPredicateSql();
    assertThat(flightSql, equalTo("F.afield != :p1"));
    String inputSql = predicate.makeInputPredicateSql();
    assertThat(inputSql, equalTo("(I.key = 'afield' AND I.value != :p1)"));

    predicate =
        new FlightFilterPredicate(
            "afield",
            FlightFilterOp.GREATER_EQUAL,
            "avalue",
            FlightFilterPredicate.Datatype.STRING,
            "p1");

    flightSql = predicate.makeFlightPredicateSql();
    assertThat(flightSql, equalTo("F.afield >= :p1"));

    predicate =
        new FlightFilterPredicate(
            "afield",
            FlightFilterOp.GREATER_THAN,
            "avalue",
            FlightFilterPredicate.Datatype.STRING,
            "p1");

    flightSql = predicate.makeFlightPredicateSql();
    assertThat(flightSql, equalTo("F.afield > :p1"));

    predicate =
        new FlightFilterPredicate(
            "afield",
            FlightFilterOp.LESS_EQUAL,
            "avalue",
            FlightFilterPredicate.Datatype.STRING,
            "p1");

    flightSql = predicate.makeFlightPredicateSql();
    assertThat(flightSql, equalTo("F.afield <= :p1"));

    predicate =
        new FlightFilterPredicate(
            "afield",
            FlightFilterOp.LESS_THAN,
            "avalue",
            FlightFilterPredicate.Datatype.STRING,
            "p1");

    flightSql = predicate.makeFlightPredicateSql();
    assertThat(flightSql, equalTo("F.afield < :p1"));
  }

  // There next 6 tests are all the possibilities for
  // query forms 1, 2, and 3 with and without flight filters
  @Test
  public void filterForm1NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    FlightFilter filter = new FlightFilter();
    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm1WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F WHERE"
            + " F.completed_time > :ff1 AND F.class_name = :ff2 AND F.status = :ff3 AND F.submit_time < :ff4"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    Instant submit = now();
    Instant complete = now();
    FlightFilter filter =
        new FlightFilter()
            .addFilterCompletedTime(FlightFilterOp.GREATER_THAN, submit)
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class)
            .addFilterFlightStatus(FlightFilterOp.EQUAL, FlightStatus.RUNNING)
            .addFilterSubmitTime(FlightFilterOp.LESS_THAN, complete);
    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm2NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F INNER JOIN flightinput I"
            + " ON F.flightid = I.flightid"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com");

    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm2WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F INNER JOIN flightinput I"
            + " ON F.flightid = I.flightid"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " AND F.class_name = :ff2"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class);

    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm3NoFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F INNER JOIN "
            + "(SELECT flightid, COUNT(*) AS matchCount FROM flightinput I"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " OR (I.key = 'name' AND I.value = :ff2)"
            + " GROUP BY I.flightid) INPUT"
            + " ON F.flightid = INPUT.flightid"
            + " WHERE INPUT.matchCount = 2"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterInputParameter("name", FlightFilterOp.EQUAL, "dd");

    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }

  @Test
  public void filterForm3WithFilterTest() throws Exception {
    String expect =
        "SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,"
            + " F.output_parameters, F.output_parameters_version, F.status, F.serialized_exception"
            + " FROM flight F INNER JOIN "
            + "(SELECT flightid, COUNT(*) AS matchCount FROM flightinput I"
            + " WHERE (I.key = 'email' AND I.value = :ff1)"
            + " OR (I.key = 'name' AND I.value = :ff2)"
            + " GROUP BY I.flightid) INPUT"
            + " ON F.flightid = INPUT.flightid"
            + " WHERE INPUT.matchCount = 2"
            + " AND F.class_name = :ff3"
            + " ORDER BY submit_time LIMIT :limit OFFSET :offset";

    FlightFilter filter =
        new FlightFilter()
            .addFilterInputParameter("email", FlightFilterOp.EQUAL, "ddtest@gmail.com")
            .addFilterInputParameter("name", FlightFilterOp.EQUAL, "dd")
            .addFilterFlightClass(FlightFilterOp.EQUAL, TestFlight.class);

    String sql = filter.makeSql();
    assertThat(sql, equalTo(expect));
  }
}
