package bio.terra.stairway;

public enum FlightFilterOp {
    EQUAL(" = "),
    NOT_EQUAL(" != "),
    GREATER_THAN(" > "),
    LESS_THAN(" < "),
    GREATER_EQUAL(" >= "),
    LESS_EQUAL(" <= ");

    final private String sql;

    FlightFilterOp(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }
}
