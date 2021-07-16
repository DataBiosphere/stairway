package bio.terra.stairway.impl;

import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.exception.FlightFilterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * A FlightFilterAccess is used to access FlightFilter members for generating
 * the SQL queries applying the predicates.
 */
public class FlightFilterAccess extends FlightFilter {

  List<FlightFilterPredicate> getFlightPredicates() {
    return flightPredicates;
  }

  List<FlightFilterPredicate> getInputPredicates() {
    return inputPredicates;
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
        storeFlightPredicateValue(predicate, statement);
      }
      for (FlightFilterPredicate predicate : inputPredicates) {
        storeInputPredicateValue(predicate, statement);
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
        .append(makeInputPredicateSql(inputPredicates.get(0)));
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
        String sql = makeFlightPredicateSql(predicate);
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
      String sql = makeInputPredicateSql(predicate);
      sb.append(inter).append(sql);
      inter = " OR ";
    }
  }

  // -- predicate methods --

  /**
   * Make the one SQL predicate for the flight table of this predicate.
   *
   * @return the SQL predicate
   */
  String makeFlightPredicateSql(FlightFilterPredicate predicate) {
    return "F." + predicate.getKey() + predicate.getOp().getSql() + ":" + predicate.getParameterName();
  }

  /**
   * Store the parameter value for substitution into the prepared statement.
   *
   * @param statement statement being readied for execution
   * @throws SQLException on errors setting the parameter values
   */
  void storeFlightPredicateValue(FlightFilterPredicate predicate, NamedParameterPreparedStatement statement) throws SQLException {
    switch (predicate.getDatatype()) {
      case STRING:
        statement.setString(predicate.getParameterName(), (String) predicate.getValue());
        break;
      case TIMESTAMP:
        statement.setInstant(predicate.getParameterName(), (Instant) predicate.getValue());
        break;
    }
  }

  /**
   * Make a SQL predicate to apply to the flight input table. The predicate looks like: {@code
   * (I.key = 'key name' AND I.value OP [placeholder])}
   *
   * @return the SQL predicate
   */
  String makeInputPredicateSql(FlightFilterPredicate predicate) {
    return "(I.key = '" + predicate.getKey() + "' AND I.value" + predicate.getOp().getSql() + ":" + predicate.getParameterName() + ")";
  }

  void storeInputPredicateValue(FlightFilterPredicate predicate, NamedParameterPreparedStatement statement)
      throws SQLException, JsonProcessingException {
    String jsonValue = StairwayMapper.getObjectMapper().writeValueAsString(predicate.getValue());
    statement.setString(predicate.getParameterName(), jsonValue);
  }

}
