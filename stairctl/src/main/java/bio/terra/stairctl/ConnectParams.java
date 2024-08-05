package bio.terra.stairctl;

import java.util.Objects;

// Container for DB connection parameters. It provides defaulting from another instance
// of ConnectParams so that we can easily apply the incoming configuration.
public record ConnectParams(
    String username, String password, String host, String port, String dbname) {

  public String makeUri() {
    return String.format("jdbc:postgresql://%s:%s/%s", host, port, dbname);
  }

  // Fill in any null parameters with those from the provided defaults
  public ConnectParams withDefaults(ConnectParams defaults) {
    return new ConnectParams(
        Objects.requireNonNullElseGet(username, defaults::username),
        Objects.requireNonNullElseGet(password, defaults::password),
        Objects.requireNonNullElseGet(host, defaults::host),
        Objects.requireNonNullElseGet(port, defaults::port),
        Objects.requireNonNullElseGet(dbname, defaults::dbname));
  }
}
