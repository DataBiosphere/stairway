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
 *         makePredicate("other_num", LESS_THAN, 456)),
 *         makeOr(
 *             makePredicate("email", EQUAL, "ddtest@gmail.com"),
 *             makePredicate("num", GREATER_THAN, 123)))
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
            "submit_time", op, timestamp, Datatype.TIMESTAMP, makeParameterName());
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
            "class_name", op, className, Datatype.STRING, makeParameterName());
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
            "status", op, status.name(), Datatype.STRING, makeParameterName());
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
            "flightid", FlightFilterOp.IN, flightIds, Datatype.LIST, makeParameterName());
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
    inputPredicates.add(makeInputPredicate(key, op, value).build(this));
    return this;
  }

  /**
   * Filter by a boolean operation. This is an OR or AND statement with a list of
   * FlightBooleanOperationExpression objects or a PREDICATE that just contains a single
   * FlightFilterPredicate (it's a wrapper to allow to nest general FlightFilterPredicate objects in
   * these boolean operations).
   *
   * @param expression The boolean operation expression to add
   * @return {@code this}, for fluent style
   * @throws FlightFilterException if predicate is not supplied
   */
  public FlightFilter setFilterInputBooleanOperationParameter(
      Function<FlightFilter, FlightBooleanOperationExpression> expression)
      throws FlightFilterException {
    if (expression == null) {
      throw new FlightFilterException("Expression must be specified in an input filter");
    }
    inputBooleanOperationExpression = expression.apply(this);
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
  public static FlightFilterPredicate.Builder makeInputPredicate(
      String key, FlightFilterOp op, Object value) {
    if (key == null) {
      throw new FlightFilterException("Key must be specified in an input filter");
    }
    if (value == null && op != FlightFilterOp.EQUAL) {
      throw new FlightFilterException(
          "Value cannot be null in an input filter if not doing an equality check");
    }

    if (op == FlightFilterOp.IN) {
      return new FlightFilterPredicate.Builder(
          key, op, value, Datatype.LIST, FlightFilter::makeParameterName);
    } else if (value == null) {
      return new FlightFilterPredicate.Builder(key, op, null, Datatype.NULL, (f) -> null);
    } else {
      return new FlightFilterPredicate.Builder(
          key, op, value, Datatype.STRING, FlightFilter::makeParameterName);
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
    private final FlightFilterOp op;
    private final String key;
    private final Object value;
    private final FlightFilterPredicate.Datatype datatype;
    private final String parameterName;
    /**
     * Predicate comparison constructor
     *
     * @param key name of the input parameter
     * @param op comparison operator
     * @param value value to compare against
     * @param datatype comparison datatype for the value
     * @param parameterName placeholder parameter name for this predicate value
     */
    FlightFilterPredicate(
        String key,
        FlightFilterOp op,
        Object value,
        FlightFilterPredicate.Datatype datatype,
        String parameterName) {
      this.key = key;
      this.op = op;
      this.value = value;
      this.datatype = datatype;
      this.parameterName = parameterName;
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

    public static class Builder implements FlightFilterPredicateInterface.Builder {
      private final FlightFilterOp op;
      private final String key;
      private final Object value;
      private final FlightFilterPredicate.Datatype datatype;
      private final Function<FlightFilter, String> parameterNameSupplier;

      public Builder(
          String key,
          FlightFilterOp op,
          Object value,
          Datatype datatype,
          Function<FlightFilter, String> parameterNameSupplier) {
        this.key = key;
        this.op = op;
        this.value = value;
        this.datatype = datatype;
        this.parameterNameSupplier = parameterNameSupplier;
      }

      public FlightFilterPredicate build(FlightFilter filter) {
        String parameterName =
            parameterNameSupplier != null ? parameterNameSupplier.apply(filter) : null;
        return new FlightFilterPredicate(key, op, value, datatype, parameterName);
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
