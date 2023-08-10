package bio.terra.stairway;

import bio.terra.stairway.FlightFilter.FlightFilterPredicate.Datatype;
import bio.terra.stairway.exception.FlightFilterException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
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
  private int parameterId;

  public FlightFilter() {
    flightPredicates = new ArrayList<>();
    inputPredicates = new ArrayList<>();
    submittedTimeSortDirection = FlightFilterSortDirection.ASC;
    parameterId = 0;
  }

  /**
   * Use this constructor to allow for more complex selection criteria. This takes in expression
   * builder methods that are created using static methods on this class.
   */
  public FlightFilter(
      FlightBooleanOperationExpression.Builder inputBooleanOperationExpressionBuilder) {
    this();
    this.inputBooleanOperationExpression = inputBooleanOperationExpressionBuilder.build(this);
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
            Datatype.TIMESTAMP,
            makeParameterName());
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
            (timestamp == null ? Datatype.NULL : Datatype.TIMESTAMP),
            makeParameterName());
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
            Datatype.STRING,
            makeParameterName());
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
            Datatype.STRING,
            makeParameterName());
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
            Datatype.LIST,
            makeParameterName());
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
    inputPredicates.add(makePredicateInput(key, op, value).build(this));
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

  /**
   * Create a filter object that will filter on a flight's submission time.
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp the flights' submission time
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateSubmitTime(
      FlightFilterOp op, @Nullable Instant timestamp) {
    return makePredicateFlight("submit_time", op, timestamp, Datatype.TIMESTAMP);
  }

  /**
   * Create a filter object that will filter on a flight's submission time.
   *
   * @param op a {@link FlightFilterOp}
   * @param timestamp the flights' submission time
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateCompletedTime(
      FlightFilterOp op, @Nullable Instant timestamp) {
    return makePredicateFlight("completed_time", op, timestamp, Datatype.TIMESTAMP);
  }

  /**
   * Create a filter object that will filter on a flight's class.
   *
   * @param op a {@link FlightFilterOp}
   * @param clazz the flights' class
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateFlightClass(
      FlightFilterOp op, Class<? extends Flight> clazz) {
    return makePredicateFlightClass(op, clazz.getName());
  }

  /**
   * Create a filter object that will filter on a flight's class.
   *
   * @param op a {@link FlightFilterOp}
   * @param className the name of the flights' class
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateFlightClass(
      FlightFilterOp op, String className) {
    return makePredicateFlight("class_name", op, className, Datatype.STRING);
  }

  /**
   * Create a predicate object that will filter on a flight's status.
   *
   * @param op a {@link FlightFilterOp}
   * @param status the flights' status
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateFlightStatus(
      FlightFilterOp op, FlightStatus status) {
    return makePredicateFlight("status", op, status.name(), Datatype.STRING);
  }

  /**
   * Create a predicate object that will filter on a flight's ids.
   *
   * @param flightIds Flight ids to filter on
   * @return A newly created FlightFilterPredicate object
   */
  public static FlightFilterPredicate.Builder makePredicateFlightIds(List<String> flightIds) {
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
  private static FlightFilterPredicate.Builder makePredicateFlight(
      String key, FlightFilterOp op, Object value, Datatype datatype) {
    return makePredicate(FlightFilterPredicate.FilterType.FLIGHT, key, op, value, datatype);
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
  public static FlightFilterPredicate.Builder makePredicateInput(
      String key, FlightFilterOp op, Object value) {
    return makePredicate(FlightFilterPredicate.FilterType.INPUT, key, op, value, Datatype.STRING);
  }

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
  private static FlightFilterPredicate.Builder makePredicate(
      FlightFilterPredicate.FilterType type,
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
      return new FlightFilterPredicate.Builder(
          type, key, op, value, Datatype.LIST, FlightFilter::makeParameterName);
    } else if (value == null) {
      return new FlightFilterPredicate.Builder(type, key, op, null, Datatype.NULL, (f) -> null);
    } else {
      return new FlightFilterPredicate.Builder(
          type, key, op, value, datatype, FlightFilter::makeParameterName);
    }
  }

  /**
   * Provide builders for expressions to be ANDed together
   *
   * @param expressions Builders for expressions. These are created with static methods on this
   *     class
   * @return A newly created expression builder
   */
  public static FlightBooleanOperationExpression.Builder makeAnd(
      FlightFilterPredicateInterface.Builder... expressions) {
    return new FlightBooleanOperationExpression.Builder(
        FlightBooleanOperationExpression.Operation.AND, expressions);
  }

  /**
   * Provide builders for expressions to be ANDed together
   *
   * @param expressions Builders for expressions. These are created with static methods on this
   *     class
   * @return A newly created expression builder
   */
  public static FlightBooleanOperationExpression.Builder makeOr(
      FlightFilterPredicateInterface.Builder... expressions) {
    return new FlightBooleanOperationExpression.Builder(
        FlightBooleanOperationExpression.Operation.OR, expressions);
  }

  private String makeParameterName() {
    parameterId++;
    return "ff" + parameterId;
  }

  public interface FlightFilterPredicateInterface {
    interface Builder {
      FlightFilterPredicateInterface build(FlightFilter filter);
    }
  }

  public static class FlightFilterPredicate implements FlightFilterPredicateInterface {
    private final FilterType type;
    private final FlightFilterOp op;
    private final String key;
    private final Object value;
    private final FlightFilterPredicate.Datatype datatype;
    private final String parameterName;
    /**
     * Predicate comparison constructor
     *
     * @param type type of filter
     * @param key name of the input parameter
     * @param op comparison operator
     * @param value value to compare against
     * @param datatype comparison datatype for the value
     * @param parameterName placeholder parameter name for this predicate value
     */
    FlightFilterPredicate(
        FilterType type,
        String key,
        FlightFilterOp op,
        Object value,
        FlightFilterPredicate.Datatype datatype,
        String parameterName) {
      this.type = type;
      this.key = key;
      this.op = op;
      this.value = value;
      this.datatype = datatype;
      this.parameterName = parameterName;
    }

    public FilterType getType() {
      return type;
    }

    public FlightFilterOp getOp() {
      return op;
    }

    public String getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }

    public FlightFilterPredicate.Datatype getDatatype() {
      return datatype;
    }

    public String getParameterName() {
      return parameterName;
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

    public static class Builder implements FlightFilterPredicateInterface.Builder {
      private final FilterType type;
      private final FlightFilterOp op;
      private final String key;
      private final Object value;
      private final FlightFilterPredicate.Datatype datatype;
      private final Function<FlightFilter, String> parameterNameSupplier;

      public Builder(
          FilterType type,
          String key,
          FlightFilterOp op,
          Object value,
          Datatype datatype,
          Function<FlightFilter, String> parameterNameSupplier) {
        this.type = type;
        this.key = key;
        this.op = op;
        this.value = value;
        this.datatype = datatype;
        this.parameterNameSupplier = parameterNameSupplier;
      }

      public FlightFilterPredicate build(FlightFilter filter) {
        String parameterName =
            parameterNameSupplier != null ? parameterNameSupplier.apply(filter) : null;
        return new FlightFilterPredicate(type, key, op, value, datatype, parameterName);
      }
    }
  }

  public static class FlightBooleanOperationExpression implements FlightFilterPredicateInterface {

    private final Operation operation;
    private final List<FlightFilterPredicateInterface> expressions;

    private FlightBooleanOperationExpression(
        Operation operation, FlightFilterPredicateInterface... expressions) {
      this(operation, List.of(expressions));
    }

    private FlightBooleanOperationExpression(
        Operation operation, List<FlightFilterPredicateInterface> expressions) {
      this.operation = operation;
      this.expressions = expressions;
    }

    public Operation getOperation() {
      return operation;
    }

    public List<FlightFilterPredicateInterface> getExpressions() {
      return expressions;
    }

    public static class Builder implements FlightFilterPredicateInterface.Builder {
      private final Operation operation;
      private final List<FlightFilterPredicateInterface.Builder> expressions;

      public Builder(Operation operation, FlightFilterPredicateInterface.Builder[] expressions) {
        this(operation, List.of(expressions));
      }

      public Builder(
          Operation operation, List<FlightFilterPredicateInterface.Builder> expressions) {
        this.operation = operation;
        this.expressions = expressions;
      }

      @Override
      public FlightBooleanOperationExpression build(FlightFilter filter) {
        return new FlightBooleanOperationExpression(
            operation, expressions.stream().map(e -> e.build(filter)).toList());
      }
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
