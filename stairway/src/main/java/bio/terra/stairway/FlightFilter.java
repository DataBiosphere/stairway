package bio.terra.stairway;

import static bio.terra.stairway.FlightFilterPredicate.Datatype.STRING;
import static bio.terra.stairway.FlightFilterPredicate.Datatype.TIMESTAMP;

import bio.terra.stairway.exception.FlightFilterException;
import bio.terra.stairway.impl.FlightDao;
import bio.terra.stairway.impl.NamedParameterPreparedStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * A FlightFilter is used to filter the flights on a flight enumeration. You can build a filter with
 * a fluent style:
 *
 * <pre>{@code
 * FlightFilter filter = new FlightFilter()
 *     .addFilterSubmitTime(GREATER_THAN, yesterday)
 *     .addFilterFlightClass(EQUAL, IngestFlight.class)
 *     .addFilterFlightStatue(EQUAL, FlightStatus.COMPLETED)
 *     .addFilterInputParameter("email", EQUAL, "ddtest@gmail.com");
 * }</pre>
 *
 * That filter would return ingest flights completed in the last day (assuming you'd properly set
 * the value of {@code yesterday}) run by user ddtest (assuming you'd passed the email as an input
 * parameter.
 */
public class FlightFilter {
  private final List<FlightFilterPredicate> flightPredicates;
  private final List<FlightFilterPredicate> inputPredicates;
  private int parameterId;

  public FlightFilter() {
    flightPredicates = new ArrayList<>();
    inputPredicates = new ArrayList<>();
    parameterId = 0;
  }

  /**
   * Filter by submit time
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp an Instant
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterSubmitTime(FlightFilterOp op, Instant timestamp) {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate("submit_time", op, timestamp, TIMESTAMP, makeParameterName());
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by completed time. Note that completed time can be NULL.
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp an Instant
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterCompletedTime(FlightFilterOp op, Instant timestamp) {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate("completed_time", op, timestamp, TIMESTAMP, makeParameterName());
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by the class name of the flight
   *
   * @param op a {@link FlightFilterOp}
   * @param clazz a class derived from the {@code Flight} class.
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterFlightClass(FlightFilterOp op, Class<? extends Flight> clazz) {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate("class_name", op, clazz.getName(), STRING, makeParameterName());
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by flight status
   *
   * @param op a {@link FlightFilterOp}
   * @param status one of the {@link FlightStatus} enumerations
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterFlightStatus(FlightFilterOp op, FlightStatus status) {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate("status", op, status.name(), STRING, makeParameterName());
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by an input parameter. This is processed by converting the {@code value} into JSON and
   * doing a string comparison against the input parameter stored in the database. The {@code value}
   * object must be <b>exactly</b> the same class as the input parameter.
   *
   * @param key name of the parameter to compare
   * @param op a {@link FlightFilterOp}
   * @param value some object for comparison
   * @return {@code this}, for fluent style
   * @throws FlightFilterException if key is not supplied
   */
  public FlightFilter addFilterInputParameter(String key, FlightFilterOp op, Object value)
      throws FlightFilterException {
    if (key == null) {
      throw new FlightFilterException("Key must be specified in an input filter");
    }
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(key, op, value, STRING, makeParameterName());
    inputPredicates.add(predicate);
    return this;
  }

  /**
   * Store the comparison values associated with the predicates into the SQL statement
   *
   * @param statement named parameter SQL statement structure
   * @throws FlightFilterException on SQL and JSON failures
   */
  void storePredicateValues(NamedParameterPreparedStatement statement)
      throws FlightFilterException {
    try {
      for (FlightFilterPredicate predicate : flightPredicates) {
        predicate.storeFlightPredicateValue(statement);
      }
      for (FlightFilterPredicate predicate : inputPredicates) {
        predicate.storeInputPredicateValue(statement);
      }
    } catch (SQLException | JsonProcessingException ex) {
      throw new FlightFilterException("Failure storing predicate values", ex);
    }
  }

  /**
   * Generate the enumeration query for flights. The list may be restricted by providing one or more
   * filters. The filters are logically ANDed together.
   *
   * <p>For performance, there are three forms of the query.
   *
   * <p><bold>Form 1: no input parameter filters</bold>
   *
   * <p>When there are no input parameter filters, then we can do a single table query on the flight
   * table like: {@code SELECT flight.* FROM flight WHERE flight-filters ORDER BY } The WHERE and
   * flight-filters are omitted if there are none to apply.
   *
   * <p><bold>Form 2: one input parameter</bold>
   *
   * <p>When there is one input filter restriction, we can do a simple join with a restricting join
   * against the input table like:
   *
   * <pre>{@code
   * SELECT flight.*
   * FROM flight JOIN flight_input
   *   ON flight.flightid = flight_input.flightid
   * WHERE flight-filters
   *   AND flight_input.key = 'key' AND flight_input.value = 'json-of-object'
   * }</pre>
   *
   * <bold>Form 3: more than one input parameter</bold>
   *
   * <p>This one gets complicated. We do a subquery that filters by the OR of the input filters and
   * groups by the COUNT of the matches. Only flights where we have inputs that qualify by the
   * filter will have the right count of matches. The query form is like:
   *
   * <pre>{@code
   * SELECT flight.*
   * FROM flight
   * JOIN (SELECT flightid, COUNT(*) AS matchCount
   *       FROM flight_input
   *       WHERE (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *          OR (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *          OR (flight_input.key = 'key1' AND flight_input.value = 'json-of-object')
   *       GROUP BY flightid) INPUT
   * ON flight.flightid = INPUT.flightid
   * WHERE flight-filters
   *   AND INPUT.matchCount = 3
   * }</pre>
   *
   * In all cases, the result is sorted and paged like this: (@code ORDER BY submit_time LIMIT
   * :limit OFFSET :offset}
   */
  String makeSql() {
    StringBuilder sb = new StringBuilder();

    // All forms start with the same select list
    sb.append("SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,")
        .append(" F.output_parameters, F.status, F.serialized_exception")
        .append(" FROM ");

    // Decide which form of the query to build.
    switch (inputPredicates.size()) {
      case 0:
        makeSqlForm1(sb);
        break;
      case 1: // Form 2
        makeSqlForm2(sb);
        break;
      default: // Form 3
        makeSqlForm3(sb);
    }

    // All forms end with the same order by, limit, and offset
    sb.append(" ORDER BY submit_time LIMIT :limit OFFSET :offset");
    return sb.toString();
  }

  // Form1: only flight table filtering
  private void makeSqlForm1(StringBuilder sb) {
    sb.append(FlightDao.FLIGHT_TABLE).append(" F");
    makeFlightFilter(sb, " WHERE ");
  }

  // Form2: flight table filtering and a single input parameter filter
  private void makeSqlForm2(StringBuilder sb) {
    sb.append(FlightDao.FLIGHT_TABLE)
        .append(" F INNER JOIN ")
        .append(FlightDao.FLIGHT_INPUT_TABLE)
        .append(" I ON F.flightid = I.flightid WHERE ")
        .append(inputPredicates.get(0).makeInputPredicateSql());
    makeFlightFilter(sb, " AND ");
  }

  // Form3: flight table filtering and more than one input parameter filter
  private void makeSqlForm3(StringBuilder sb) {
    sb.append(FlightDao.FLIGHT_TABLE)
        .append(" F INNER JOIN (SELECT flightid, COUNT(*) AS matchCount")
        .append(" FROM ")
        .append(FlightDao.FLIGHT_INPUT_TABLE)
        .append(" I WHERE ");
    makeInputFilter(sb);
    sb.append(" GROUP BY I.flightid) INPUT ON F.flightid = INPUT.flightid")
        .append(" WHERE INPUT.matchCount = ")
        .append(inputPredicates.size());
    makeFlightFilter(sb, " AND ");
  }

  /**
   * Generate the filter for the flight table. The right hand side of any comparison op is generated
   * as a named parameter.
   */
  private void makeFlightFilter(StringBuilder sb, String prefix) {
    if (flightPredicates.size() > 0) {
      sb.append(prefix);
      String inter = StringUtils.EMPTY;

      for (FlightFilterPredicate predicate : flightPredicates) {
        String sql = predicate.makeFlightPredicateSql();
        sb.append(inter).append(sql);
        inter = " AND ";
      }
    }
  }

  /**
   * Generate the filter for the flight input table for Form 3: the case where there is more that
   * one predicate.
   */
  private void makeInputFilter(StringBuilder sb) {
    String inter = StringUtils.EMPTY;

    for (FlightFilterPredicate predicate : inputPredicates) {
      String sql = predicate.makeInputPredicateSql();
      sb.append(inter).append(sql);
      inter = " OR ";
    }
  }

  private String makeParameterName() {
    parameterId++;
    return "ff" + parameterId;
  }
}
