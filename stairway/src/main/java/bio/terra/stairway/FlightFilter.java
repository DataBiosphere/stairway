package bio.terra.stairway;

import bio.terra.stairway.FlightFilter.FlightFilterPredicate.Datatype;
import bio.terra.stairway.exception.FlightFilterException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
  protected final List<FlightFilterPredicate> flightPredicates;
  protected final List<FlightFilterPredicate> inputPredicates;
  protected int parameterId;

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
        new FlightFilterPredicate(
            "submit_time", op, timestamp, Datatype.TIMESTAMP, makeParameterName());
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
        new FlightFilterPredicate(
            "completed_time", op, timestamp, Datatype.TIMESTAMP, makeParameterName());
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
        new FlightFilterPredicate(
            "class_name", op, clazz.getName(), Datatype.STRING, makeParameterName());
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
        new FlightFilterPredicate(
            "status", op, status.name(), Datatype.STRING, makeParameterName());
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
        new FlightFilterPredicate(key, op, value, Datatype.STRING, makeParameterName());
    inputPredicates.add(predicate);
    return this;
  }

  private String makeParameterName() {
    parameterId++;
    return "ff" + parameterId;
  }

  public static class FlightFilterPredicate {
    public enum Datatype {
      STRING,
      TIMESTAMP
    }

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
  }
}
