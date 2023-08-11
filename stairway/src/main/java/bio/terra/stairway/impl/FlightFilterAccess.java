package bio.terra.stairway.impl;

import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilter.FlightBooleanOperationExpression;
import bio.terra.stairway.FlightFilter.FlightFilterPredicate;
import bio.terra.stairway.FlightFilter.FlightFilterPredicate.Datatype;
import bio.terra.stairway.FlightFilter.FlightFilterPredicateInterface;
import bio.terra.stairway.FlightFilterSortDirection;
import bio.terra.stairway.StairwayMapper;
import bio.terra.stairway.exception.FlightFilterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
  private final Map<FlightFilterPredicate, String> predicateToParameterName = new HashMap<>();

  private int parameterId;

  public FlightFilterAccess(
      FlightFilter filter, Integer offset, Integer limit, String pageTokenString) {
    this.filter = filter;
    this.offset = offset;
    this.limit = limit;
    this.pageToken = Optional.ofNullable(pageTokenString).map(PageToken::new).orElse(null);
  }

  private String getParameterName(FlightFilterPredicate predicate) {
    return predicateToParameterName.computeIfAbsent(predicate, p -> "ff" + ++parameterId);
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

      if (filter.getInputBooleanOperationExpression() != null) {
        storeInputPredicateValue(filter.getInputBooleanOperationExpression(), statement);
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
   * <p>Input parameter filters are ANDed together by default or can be organized using
   * FlightBooleanOperationExpression objects. The generated SQL might look like
   *
   * <pre>{@code
   * SELECT *
   * FROM flight
   * WHERE (1=1)
   * AND EXISTS (SELECT 0 FROM flightinput I
   *             WHERE F.flightid=I.flightid
   *             AND I.key = 'key'
   *             AND I.value = 'json-of-object')
   * }</pre>
   *
   * <p>This format allows for more complex boolean filtering logic such as
   *
   * <pre>{@code
   * SELECT *
   * FROM flight
   * WHERE (1=1)
   * AND (EXISTS (SELECT 0 FROM flightinput I
   *             WHERE F.flightid=I.flightid
   *             AND I.key = 'key1'
   *             AND I.value = 'json-of-object1')
   *   OR EXISTS (SELECT 0 FROM flightinput I
   *             WHERE F.flightid=I.flightid
   *             AND I.key = 'key2'
   *             AND I.value = 'json-of-object2'))
   * AND EXISTS (SELECT 0 FROM flightinput I
   *             WHERE F.flightid=I.flightid
   *             AND I.key = 'key3'
   *             AND I.value = 'json-of-object3)
   * }</pre>
   *
   * <p>The result is sorted like this: {@code ORDER BY submit_time ASC|DESC}. If limit is present,
   * then the {@code LIMIT :limit} is included. If offset is present, then the {@code OFFSET
   * :offset} is included.
   */
  String makeSql() {
    StringBuilder sb = new StringBuilder();

    // Start with the select list
    sb.append("SELECT F.flightid, F.stairway_id, F.submit_time, F.completed_time,")
        .append(" F.output_parameters, F.status, F.serialized_exception, F.class_name")
        .append(" FROM ");

    makeSqlForm(sb);

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
    makeSqlForm(sb);
    return sb.toString();
  }

  private void makeSqlForm(StringBuilder sb) {
    sb.append(FlightDao.FLIGHT_TABLE).append(" F WHERE (1=1)");

    for (FlightFilterPredicate predicate : filter.getInputPredicates()) {
      sb.append(" AND ").append(makeInputPredicateSql(predicate));
    }

    if (filter.getInputBooleanOperationExpression() != null) {
      sb.append(" AND ")
          .append(makeBooleanExpressionsFilters(filter.getInputBooleanOperationExpression()));
    }

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

  // -- boolean expression methods
  private String makeBooleanExpressionsFilters(FlightFilterPredicateInterface expression) {
    if (expression instanceof FlightFilterPredicate expressionAsPredicate) {
      switch (expressionAsPredicate.type()) {
        case INPUT -> {
          return makeInputPredicateSql(expressionAsPredicate);
        }
        case FLIGHT -> {
          return makeFlightPredicateSql(expressionAsPredicate);
        }
        default -> throw new FlightFilterException(
            "Unrecognized predicate type: %s".formatted(expressionAsPredicate.type()));
      }
    } else if (expression instanceof FlightBooleanOperationExpression expressionAsBooleanOp) {
      return expressionAsBooleanOp.expressions().stream()
          .map(this::makeBooleanExpressionsFilters)
          .collect(Collectors.joining(expressionAsBooleanOp.operation().getSql(), "(", ")"));
    } else {
      throw new FlightFilterException(
          "Unrecognized filter class: %s".formatted(expression.getClass().getName()));
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
    if (predicate.datatype() == Datatype.NULL) {
      return "F." + predicate.key() + " IS NULL";
    }
    if (predicate.datatype() == Datatype.LIST) {
      return "F."
          + predicate.key()
          + " "
          + predicate.op().getSql()
          + " (SELECT JSON_ARRAY_ELEMENTS_TEXT(CAST(:"
          + getParameterName(predicate)
          + " AS JSON)))";
    }
    return "F."
        + predicate.key()
        + predicate.op().getSql()
        + ":"
        + getParameterName(predicate);
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
    switch (predicate.datatype()) {
      case STRING:
        statement.setString(getParameterName(predicate), (String) predicate.value());
        break;
      case LIST:
        statement.setString(
                getParameterName(predicate), pgJsonMapper.writeValueAsString(predicate.value()));
        break;
      case TIMESTAMP:
        statement.setInstant(getParameterName(predicate), (Instant) predicate.value());
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
    if (predicate.datatype() == Datatype.NULL) {
      return "(EXISTS (SELECT 0 FROM flightinput I WHERE F.flightid = I.flightid AND I.key = "
          + "'"
          + predicate.key()
          + "' AND I.value IS NULL) "
          + "OR NOT EXISTS (SELECT 0 FROM flightinput I WHERE F.flightid = I.flightid AND I.key = "
          + "'"
          + predicate.key()
          + "'))";
    } else if (predicate.datatype() == Datatype.LIST) {
      valuePredicate =
          "I.value "
              + predicate.op().getSql()
              + " (SELECT JSON_ARRAY_ELEMENTS_TEXT(CAST(:"
              + getParameterName(predicate)
              + " AS JSON)))";
    } else {
      valuePredicate = "I.value" + predicate.op().getSql() + ":" + getParameterName(predicate);
    }

    return "EXISTS (SELECT 0 FROM flightinput I WHERE F.flightid = I.flightid AND I.key = "
        + "'"
        + predicate.key()
        + "' AND "
        + valuePredicate
        + ")";
  }

  private void storeInputPredicateValue(
      FlightBooleanOperationExpression booleanExpression, NamedParameterPreparedStatement statement)
      throws SQLException, JsonProcessingException {

    for (FlightFilterPredicateInterface expression : booleanExpression.expressions()) {
      if (expression instanceof FlightFilterPredicate expressionAsPredicate) {
        switch (expressionAsPredicate.type()) {
          case INPUT -> storeInputPredicateValue(expressionAsPredicate, statement);
          case FLIGHT -> storeFlightPredicateValue(expressionAsPredicate, statement);
          default -> throw new FlightFilterException(
              "Unrecognized predicate type: %s".formatted(expressionAsPredicate.type()));
        }
      } else if (expression instanceof FlightBooleanOperationExpression expressionAsBooleanOp) {
        storeInputPredicateValue(expressionAsBooleanOp, statement);
      } else {
        throw new FlightFilterException(
            "Unrecognized filter class: %s".formatted(expression.getClass().getName()));
      }
    }
  }

  private void storeInputPredicateValue(
      FlightFilterPredicate predicate, NamedParameterPreparedStatement statement)
      throws SQLException, JsonProcessingException {
    if (predicate.datatype() == Datatype.NULL) {
      return;
    }
    String jsonValue;
    if (predicate.datatype() == Datatype.LIST) {
      List<String> values = new ArrayList<>();
      for (Object value : ((List<?>) predicate.value())) {
        // Need to double encode in order for strings to match
        values.add(
            StairwayMapper.getObjectMapper()
                .writeValueAsString(StairwayMapper.getObjectMapper().writeValueAsString(value)));
      }
      jsonValue = "[" + String.join(",", values) + "]";
    } else {
      jsonValue = StairwayMapper.getObjectMapper().writeValueAsString(predicate.value());
    }
    statement.setString(getParameterName(predicate), jsonValue);
  }
}
