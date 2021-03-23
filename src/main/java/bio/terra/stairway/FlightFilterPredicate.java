package bio.terra.stairway;

import java.sql.SQLException;
import java.time.Instant;

class FlightFilterPredicate {
  public enum Datatype {
    STRING,
    TIMESTAMP
  }

  private final FlightFilterOp op;
  private final String key;
  private final Object value;
  private final Datatype datatype;
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
      String key, FlightFilterOp op, Object value, Datatype datatype, String parameterName) {
    this.key = key;
    this.op = op;
    this.value = value;
    this.datatype = datatype;
    this.parameterName = parameterName;
  }

  /**
   * Make the one SQL predicate for the flight table of this predicate.
   *
   * @return the SQL predicate
   */
  String makeFlightPredicateSql() {
    return "F." + key + op.getSql() + ":" + parameterName;
  }

  /**
   * Store the parameter value for substitution into the prepared statement.
   *
   * @param statement statement being readied for execution
   * @throws SQLException on errors setting the parameter values
   */
  void storeFlightPredicateValue(NamedParameterPreparedStatement statement) throws SQLException {
    switch (datatype) {
      case STRING:
        statement.setString(parameterName, (String) value);
        break;
      case TIMESTAMP:
        statement.setInstant(parameterName, (Instant) value);
        break;
    }
  }

  /**
   * Make a SQL predicate to apply to the flight input table. The predicate looks like: {@code
   * (I.key = 'key name' AND I.value OP [placeholder])}
   *
   * @return the SQL predicate
   */
  String makeInputPredicateSql() {
    return "(I.key = '" + key + "' AND I.value" + op.getSql() + ":" + parameterName + ")";
  }

  void storeInputPredicateValue(NamedParameterPreparedStatement statement) throws SQLException {
    statement.setString(parameterName, (String) value);
  }
}
