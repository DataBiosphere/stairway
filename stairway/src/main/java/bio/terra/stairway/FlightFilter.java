package bio.terra.stairway;

import bio.terra.stairway.FlightFilter.FlightFilterPredicate.Datatype;
import bio.terra.stairway.exception.FlightFilterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A FlightFilter is used to filter the flights on a flight enumeration. You can build a filter with
 * a fluent style:
 *
 * <pre>{@code
 * FlightFilter filter = new FlightFilter()
 *     .addFilterSubmitTime(GREATER_THAN, yesterday)
 *     .addFilterFlightClass(EQUAL, IngestFlight.class)
 *     .addFilterFlightStatus(EQUAL, FlightStatus.COMPLETED)
 *     .addFilterInputParameter("email", EQUAL, "ddtest@gmail.com");
 *     .submittedTimeSortDirection(FlightFilterSortDirection.DESC)
 * }</pre>
 *
 * That filter would return ingest flights completed in the last day (assuming you'd properly set
 * the value of {@code yesterday}) run by user ddtest (assuming you'd passed the email as an input
 * parameter.
 *
 * <p>You can also initialize the filter with more complex boolean filters:
 *
 * <pre>{@code
 * FlightFilter filter = new FlightFilter(
 *     makeAnd(
 *         makePredicateFlightStatus(EQUAL, FlightStatus.ERROR),
 *         makePredicateInput("other_num", LESS_THAN, 456)),
 *         makeOr(
 *             makePredicateInput("email", EQUAL, "ddtest@gmail.com"),
 *             makePredicateInput("num", GREATER_THAN, 123)))
 *     .submittedTimeSortDirection(FlightFilterSortDirection.DESC)
 * }</pre>
 */
public class FlightFilter {
  private final List<FlightFilterPredicate> flightPredicates;
  private final List<FlightFilterPredicate> inputPredicates;
  private FlightBooleanOperationExpression inputBooleanOperationExpression;
  private FlightFilterSortDirection submittedTimeSortDirection;

  // Mapper should be used to deserialize to generic Postgres JSON
  private static final ObjectMapper pgJsonMapper = new ObjectMapper();

  public FlightFilter() {
    flightPredicates = new ArrayList<>();
    inputPredicates = new ArrayList<>();
    submittedTimeSortDirection = FlightFilterSortDirection.ASC;
  }

  /**
   * Use this constructor to allow for more complex selection criteria. This takes in expression
   * builder methods that are created using static methods on this class.
   */
  public FlightFilter(FlightBooleanOperationExpression inputBooleanOperationExpression) {
    this();
    this.inputBooleanOperationExpression = inputBooleanOperationExpression;
  }

  public List<FlightFilterPredicate> getFlightPredicates() {
    return flightPredicates;
  }

  public List<FlightFilterPredicate> getInputPredicates() {
    return inputPredicates;
  }

  public FlightBooleanOperationExpression getInputBooleanOperationExpression() {
    return inputBooleanOperationExpression;
  }

  public FlightFilterSortDirection getSubmittedTimeSortDirection() {
    return submittedTimeSortDirection;
  }

  /**
   * Filter by submit time
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp an Instant
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterSubmitTime(FlightFilterOp op, Instant timestamp) {
    if (timestamp == null) {
      throw new FlightFilterException("Submit filter timestamp cannot be null");
    }
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            FlightFilterPredicate.FilterType.FLIGHT,
            "submit_time",
            op,
            timestamp,
            Datatype.TIMESTAMP);
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by completed time. Passing null for the timestamp filters for incomplete flights
   * regardless of the filter operand.
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp an Instant
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterCompletedTime(FlightFilterOp op, @Nullable Instant timestamp) {
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            FlightFilterPredicate.FilterType.FLIGHT,
            "completed_time",
            op,
            timestamp,
            (timestamp == null ? Datatype.NULL : Datatype.TIMESTAMP));
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
    return addFilterFlightClass(op, clazz.getName());
  }

  /**
   * Filter by the class name of the flight. The string class name must be equivalent to the result
   * of {@code class.getName()}; that is, the full flight class name.
   *
   * @param op a {@link FlightFilterOp}
   * @param className name of the class to filter
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterFlightClass(FlightFilterOp op, String className) {
    if (className == null) {
      throw new FlightFilterException("Class name cannot be null");
    }
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            FlightFilterPredicate.FilterType.FLIGHT,
            "class_name",
            op,
            className,
            Datatype.STRING);
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
    if (status == null) {
      throw new FlightFilterException("Status cannot be null");
    }
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            FlightFilterPredicate.FilterType.FLIGHT,
            "status",
            op,
            status.name(),
            Datatype.STRING);
    flightPredicates.add(predicate);
    return this;
  }

  /**
   * Filter by flight ids
   *
   * @param flightIds A list of flight ids to filter by
   * @return {@code this}, for fluent style
   */
  public FlightFilter addFilterFlightIds(List<String> flightIds) {
    if (flightIds == null) {
      throw new FlightFilterException("flightIds can not be null");
    }
    FlightFilterPredicate predicate =
        new FlightFilterPredicate(
            FlightFilterPredicate.FilterType.FLIGHT,
            "flightid",
            FlightFilterOp.IN,
            flightIds,
            Datatype.LIST);
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
    inputPredicates.add(FlightFilterPredicate.makePredicateInput(key, op, value));
    return this;
  }

  /**
   * Specify the sort order based on submitted time for the returned values
   *
   * @param submittedTimeSortDirection wether to sort in ascending (default) or descending order
   * @return {@code this}, for fluent style
   * @throws FlightFilterException if the specified value is null
   */
  public FlightFilter submittedTimeSortDirection(
      FlightFilterSortDirection submittedTimeSortDirection) {
    if (submittedTimeSortDirection == null) {
      throw new FlightFilterException("Sort direction cannot be null");
    }

    this.submittedTimeSortDirection = submittedTimeSortDirection;
    return this;
  }

  public record Value(FlightFilterPredicate predicate, String string, Instant instant) {
    Value(FlightFilterPredicate predicate, String string) {
      this(predicate, string, null);
    }

    Value(FlightFilterPredicate predicate, Instant instant) {
      this(predicate, null, instant);
    }

    Value() {
      this(null, null, null);
    }
  }

  public List<Value> getValues() throws JsonProcessingException {
    List<Value> values = new ArrayList<>();
    for (FlightFilterPredicate predicate : flightPredicates) {
      values.addAll(predicate.getValues());
    }
    for (FlightFilterPredicate predicate : inputPredicates) {
      values.addAll(predicate.getValues());
    }

    if (inputBooleanOperationExpression != null) {
      values.addAll(inputBooleanOperationExpression.getValues());
    }
    return values;
  }

  public interface FlightFilterPredicateInterface {
    List<Value> getValues() throws JsonProcessingException;
  }

  /**
   * Predicate comparison constructor
   *
   * @param type type of filter
   * @param key name of the input parameter
   * @param op comparison operator
   * @param value value to compare against
   * @param datatype comparison datatype for the value
   */
  public record FlightFilterPredicate(
          FilterType type,
          String key,
          FlightFilterOp op,
          Object value,
          FlightFilterPredicate.Datatype datatype) implements FlightFilterPredicateInterface {

    /**
     * Create a predicate object.
     *
     * @param type the type of predicate
     * @param key name of the parameter to compare
     * @param op a {@link FlightFilterOp}
     * @param value some object for comparison
     * @param datatype comparison datatype for the value
     * @return A newly created FlightFilterPredicate object
     */
    private static FlightFilterPredicate makePredicate(
            FilterType type,
            String key,
            FlightFilterOp op,
            Object value,
            Datatype datatype) {
      if (key == null) {
        throw new FlightFilterException("Key must be specified in an input filter");
      }
      if (value == null && op != FlightFilterOp.EQUAL) {
        throw new FlightFilterException(
            "Value cannot be null in an input filter if not doing an equality check");
      }

      if (op == FlightFilterOp.IN) {
        return new FlightFilterPredicate(type, key, op, value, Datatype.LIST);
      } else if (value == null) {
        return new FlightFilterPredicate(type, key, op, null, Datatype.NULL);
      } else {
        return new FlightFilterPredicate(type, key, op, value, datatype);
      }
    }

    /**
     * Create an input parameter filter object. This is processed by converting the {@code value} into
     * JSON and doing a string comparison against the input parameter stored in the database. The
     * {@code value} object must be <b>exactly</b> the same class as the input parameter.
     *
     * @param key name of the parameter to compare
     * @param op a {@link FlightFilterOp}
     * @param value some object for comparison
     * @return A newly created FlightFilterPredicate object
     * @throws FlightFilterException if predicate is not supplied
     */
    public static FlightFilterPredicate makePredicateInput(String key, FlightFilterOp op, Object value) {
      return makePredicate(FilterType.INPUT, key, op, value, Datatype.STRING);
    }

    /**
     * Create a predicate object that will filter on a flight's status.
     *
     * @param op a {@link FlightFilterOp}
     * @param status the flights' status
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateFlightStatus(FlightFilterOp op, FlightStatus status) {
      return makePredicateFlight("status", op, status.name(), Datatype.STRING);
    }

    /**
     * Create a filter object that will filter on a flight's class.
     *
     * @param op a {@link FlightFilterOp}
     * @param className the name of the flights' class
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateFlightClass(FlightFilterOp op, String className) {
      return makePredicateFlight("class_name", op, className, Datatype.STRING);
    }

    /**
     * Create a predicate object that will filter on a flight's ids.
     *
     * @param flightIds Flight ids to filter on
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateFlightIds(List<String> flightIds) {
      return makePredicateFlight("flightid", FlightFilterOp.IN, flightIds, Datatype.LIST);
    }

    /**
     * Create a predicate object that will filter on a flight level parameter.
     *
     * @param key name of the parameter to compare
     * @param op a {@link FlightFilterOp}
     * @param value some object for comparison
     * @param datatype comparison datatype for the value
     * @return A newly created FlightFilterPredicate object
     */
    private static FlightFilterPredicate makePredicateFlight(
            String key, FlightFilterOp op, Object value, Datatype datatype) {
      return makePredicate(FilterType.FLIGHT, key, op, value, datatype);
    }

    /**
     * Create a filter object that will filter on a flight's class.
     *
     * @param op a {@link FlightFilterOp}
     * @param clazz the flights' class
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateFlightClass(
            FlightFilterOp op, Class<? extends Flight> clazz) {
      return makePredicateFlightClass(op, clazz.getName());
    }

    /**
     * Create a filter object that will filter on a flight's submission time.
     *
     * @param op a {@link FlightFilterOp}
     * @param timestamp the flights' submission time
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateCompletedTime(
            FlightFilterOp op, @Nullable Instant timestamp) {
      return makePredicateFlight("completed_time", op, timestamp, Datatype.TIMESTAMP);
    }

    /**
     * Create a filter object that will filter on a flight's submission time.
     *
     * @param op a {@link FlightFilterOp}
     * @param timestamp the flights' submission time
     * @return A newly created FlightFilterPredicate object
     */
    public static FlightFilterPredicate makePredicateSubmitTime(
            FlightFilterOp op, @Nullable Instant timestamp) {
      return makePredicateFlight("submit_time", op, timestamp, Datatype.TIMESTAMP);
    }

    @Override
    public List<Value> getValues() throws JsonProcessingException {
      return List.of(switch (type) {
        case INPUT -> getInputValues();
        case FLIGHT -> getFlightValues();
      });
    }

    private Value getInputValues() throws JsonProcessingException {
      return switch (datatype) {
      case LIST -> {
        List<String> values = new ArrayList<>();
        for (Object obj : ((List<?>) value)) {
          // Need to double encode in order for strings to match
          values.add(
                  StairwayMapper.getObjectMapper()
                          .writeValueAsString(StairwayMapper.getObjectMapper().writeValueAsString(obj)));
        }
        yield new Value(this, "[" + String.join(",", values) + "]");
      }
      case NULL -> new Value();
      default -> new Value(this, StairwayMapper.getObjectMapper().writeValueAsString(value));
      };
    }

    /**
     * Get the parameter value for substitution into the prepared statement.
     */
    private Value getFlightValues() throws JsonProcessingException {
      return switch (datatype) {
        case STRING -> new Value(this, (String) value);
        case LIST -> new Value(this, pgJsonMapper.writeValueAsString(value));
        case TIMESTAMP -> new Value(this, (Instant) value);
        // Ignore the parameter in the null case
        case NULL -> new Value();
      };
    }

    public enum Datatype {
      STRING,
      TIMESTAMP,
      LIST,
      NULL
    }

    public enum FilterType {
      FLIGHT, // Does this filter apply to flight level attributes
      INPUT // Does this filter apply to flight input level attributes
    }
  }

    /**
     * A predicate that is a boolean operation of other predicates
     * @param operation The operation to perform
     * @param expressions Expression that will have the boolean operator applied to them
     */
  public record FlightBooleanOperationExpression(
          Operation operation, List<FlightFilterPredicateInterface> expressions) implements FlightFilterPredicateInterface {
    /**
     * @param expressions Expressions that will be AND-ed together.
     */
    public static FlightBooleanOperationExpression makeAnd(FlightFilterPredicateInterface... expressions) {
      return new FlightBooleanOperationExpression(Operation.AND, List.of(expressions));
    }

    /**
     * @param expressions Expressions that will be OR-ed together.
     */
    public static FlightBooleanOperationExpression makeOr(FlightFilterPredicateInterface... expressions) {
      return new FlightBooleanOperationExpression(Operation.OR, List.of(expressions));
    }

    @Override
    public List<Value> getValues() throws JsonProcessingException {
      List<Value> values = new ArrayList<>();
      for (FlightFilterPredicateInterface expression : expressions()) {
        values.addAll(expression.getValues());
      }
      return values;
    }

    public enum Operation {
      AND(" AND "),
      OR(" OR ");

      private final String sql;

      Operation(String sql) {
        this.sql = sql;
      }

      public String getSql() {
        return sql;
      }
    }
  }
}
