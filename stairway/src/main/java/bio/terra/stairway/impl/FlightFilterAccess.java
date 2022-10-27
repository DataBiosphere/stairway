package bio.terra.stairway.impl;

import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilter.FlightFilterPredicate;
import bio.terra.stairway.FlightFilter.FlightFilterPredicate.Datatype;
import bio.terra.stairway.FlightFilterSortDirection;
import bio.terra.stairway.StairwayMapper;
import bio.terra.stairway.exception.FlightFilterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

/**
 * A FlightFilterAccess is used to access FlightFilter members for generating the SQL queries
 * applying the predicates.
 *
 * <p>We use this class in two ways. First, to count all of the rows in the filtered set, we pass
 * null values for all of the paging controls (offset, limit, pageToken). Then call {@link
 * #makeCountSql()} to get the COUNT(*) select. Second, we pass the provided values for the paging
 * controls and then call {@link #makeSql} to get the full select list and the paging code gen.
 */
class FlightFilterAccess {
  private final FlightFilter filter;
  private final Integer offset;
  private final Integer limit;
  private final PageToken pageToken;
  // Mapper should be used to deserialize to generic Postgres JSON
  private final ObjectMapper pgJsonMapper = new ObjectMapper();

  public FlightFilterAccess(
      FlightFilter filter, Integer offset, Integer limit, String pageTokenString) {
    this.filter = filter;
    this.offset = offset;
    this.limit = limit;
    this.pageToken = Optional.ofNullable(pageTokenString).map(PageToken::new).orElse(null);
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
      for (FlightFilterPredicate predicate : filter.getFlightPredicates()) {
        storeFlightPredicateValue(predicate, statement);
      }
      for (FlightFilterPredicate predicate : filter.getInputPredicates()) {
        storeInputPredicateValue(predicate, statement);
      }
      // Add any values for paging controls we have
      if (pageToken != null) {
        statement.setInstant("pagetoken", pageToken.getTimestamp());
      }
      if (limit != null) {
        statement.setInt("limit", limit);
      }
      if (offset != null) {
        statement.setInt("offset", offset);
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
   * <p>In all cases, the result is sorted like this: {@code ORDER BY submit_time}. If limit is
   * present, then the {@code LIMIT :limit} is included. If offset is present, then the {@code
   * OFFSET :offset} is included.
   */
  String makeSql() {
    StringBuilder sb = new StringBuilder();

    // Start with the select list
    sb.append("SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,")
        .append(" F.output_parameters, F.status, F.serialized_exception, F.class_name")
        .append(" FROM ");

    makeSqlQueryCommon(sb);

    // All forms end with the same order by with the variance being ascending or descending order
    sb.append(" ORDER BY submit_time")
        .append(" ")
        .append(filter.getSubmittedTimeSortDirection().getSql());

    // Add the paging controls if present
    if (limit != null) {
      sb.append(" LIMIT :limit");
    }
    if (offset != null) {
      sb.append(" OFFSET :offset");
    }
    return sb.toString();
  }

  /**
   * The query structure for the count query is the same as the main query. However, this query does
   * not apply the limit and offset, since we want to count the total rows that make it through the
   * query. Also leaves out order by, since that is meaningless for the aggregate.
   *
   * @return SQL string returning the count
   */
  String makeCountSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT CURRENT_TIMESTAMP AS currenttime, COUNT(*) AS totalflights FROM ");
    makeSqlQueryCommon(sb);
    return sb.toString();
  }

  // Make the common form of the query string
  private void makeSqlQueryCommon(StringBuilder sb) {
    // Decide which form of the query to build.
    switch (filter.getInputPredicates().size()) {
      case 0: // Form 1
        makeSqlForm1(sb);
        break;
      case 1: // Form 2
        makeSqlForm2(sb);
        break;
      default: // Form 3
        makeSqlForm3(sb);
    }
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
        .append(makeInputPredicateSql(filter.getInputPredicates().get(0)));
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
        .append(filter.getInputPredicates().size());
    makeFlightFilter(sb, " AND ");
  }

  /**
   * Generate the filter for the flight table. The right hand side of any comparison op is generated
   * as a named parameter.
   */
  private void makeFlightFilter(StringBuilder sb, String prefix) {
    // If we have a predicate to make...
    if (filter.getFlightPredicates().size() > 0 || pageToken != null) {
      sb.append(prefix);
      String inter = StringUtils.EMPTY;

      for (FlightFilterPredicate predicate : filter.getFlightPredicates()) {
        String sql = makeFlightPredicateSql(predicate);
        sb.append(inter).append(sql);
        inter = " AND ";
      }

      // Add the page token paging control if present
      if (pageToken != null) {
        if (filter.getSubmittedTimeSortDirection() == FlightFilterSortDirection.ASC) {
          sb.append(inter).append("F.submit_time > :pagetoken");
        } else {
          sb.append(inter).append("F.submit_time < :pagetoken");
        }
      }
    }
  }

  /**
   * Generate the filter for the flight input table for Form 3: the case where there is more that
   * one predicate.
   */
  private void makeInputFilter(StringBuilder sb) {
    String inter = StringUtils.EMPTY;

    for (FlightFilterPredicate predicate : filter.getInputPredicates()) {
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
  @VisibleForTesting
  String makeFlightPredicateSql(FlightFilterPredicate predicate) {
    if (predicate.getDatatype() == Datatype.NULL) {
      return "F." + predicate.getKey() + " IS NULL";
    }
    if (predicate.getDatatype() == Datatype.LIST) {
      return "F." + predicate.getKey() + " " + predicate.getOp().getSql()
              + " (SELECT JSON_ARRAY_ELEMENTS_TEXT(CAST(:" + predicate.getParameterName() + " AS JSON)))";
    }
    return "F."
        + predicate.getKey()
        + predicate.getOp().getSql()
        + ":"
        + predicate.getParameterName();
  }

  /**
   * Store the parameter value for substitution into the prepared statement.
   *
   * @param statement statement being readied for execution
   * @throws SQLException on errors setting the parameter values
   */
  private void storeFlightPredicateValue(
      FlightFilterPredicate predicate, NamedParameterPreparedStatement statement)
        throws SQLException, JsonProcessingException {
    switch (predicate.getDatatype()) {
      case STRING:
        statement.setString(predicate.getParameterName(), (String) predicate.getValue());
        break;
      case LIST:
        statement.setString(predicate.getParameterName(), pgJsonMapper.writeValueAsString(predicate.getValue()));
        break;
      case TIMESTAMP:
        statement.setInstant(predicate.getParameterName(), (Instant) predicate.getValue());
        break;
      case NULL:
        // Ignore the parameter in the null case
        break;
    }
  }

  /**
   * Make a SQL predicate to apply to the flight input table. The predicate looks like: {@code
   * (I.key = 'key name' AND I.value OP [placeholder])}
   *
   * @return the SQL predicate
   */
  @VisibleForTesting
  String makeInputPredicateSql(FlightFilterPredicate predicate) {
    String valuePredicate = "";
    if (predicate.getDatatype() == Datatype.NULL) {
      return "I.value IS NULL";
    } else if (predicate.getDatatype() == Datatype.LIST) {
      valuePredicate = "I.value " + predicate.getOp().getSql()
              + " (SELECT JSON_ARRAY_ELEMENTS_TEXT(CAST(:" + predicate.getParameterName() + " AS JSON)))";
    } else {
      valuePredicate = "I.value" + predicate.getOp().getSql() + ":" + predicate.getParameterName();
    }

    return "(I.key = '"
        + predicate.getKey()
        + "' AND " + valuePredicate +")";
  }

  private void storeInputPredicateValue(
      FlightFilterPredicate predicate, NamedParameterPreparedStatement statement)
      throws SQLException, JsonProcessingException {

    String jsonValue;
    if (predicate.getDatatype() == Datatype.LIST) {
      List<String> values = new ArrayList<>();
      for (Object value : ((List<?>) predicate.getValue())) {
        // Need to double encode in order for strings to match
        values.add(StairwayMapper.getObjectMapper()
                .writeValueAsString(StairwayMapper.getObjectMapper().writeValueAsString(value)));
      }
      jsonValue = "[" + StringUtils.join(values, ",") + "]";
    } else {
      jsonValue = StairwayMapper.getObjectMapper().writeValueAsString(predicate.getValue());
    }
    statement.setString(predicate.getParameterName(), jsonValue);
  }
}
