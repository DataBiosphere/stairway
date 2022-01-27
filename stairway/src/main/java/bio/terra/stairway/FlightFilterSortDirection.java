package bio.terra.stairway;

public enum FlightFilterSortDirection {
  ASC("ASC"),
  DESC("DESC");

  private final String sql;

  FlightFilterSortDirection(String sql) {
    this.sql = sql;
  }

  public String getSql() {
    return sql;
  }
}
