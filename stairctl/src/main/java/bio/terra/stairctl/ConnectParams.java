package bio.terra.stairctl;

import java.util.Optional;

// Container for DB connection parameters. It provides defaulting from another instance
// of ConnectParams so that we can easily apply the incoming configuration.
public class ConnectParams {
  private String username;
  private String password;
  private String host;
  private String port;
  private String dbname;

  public String getUsername() {
    return username;
  }

  public ConnectParams username(String username) {
    this.username = username;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public ConnectParams password(String password) {
    this.password = password;
    return this;
  }

  public String getHost() {
    return host;
  }

  public ConnectParams host(String host) {
    this.host = host;
    return this;
  }

  public String getPort() {
    return port;
  }

  public ConnectParams port(String port) {
    this.port = port;
    return this;
  }

  public String getDbname() {
    return dbname;
  }

  public ConnectParams dbname(String dbname) {
    this.dbname = dbname;
    return this;
  }

  public String makeUri() {
    return String.format("jdbc:postgresql://%s:%s/%s", host, port, dbname);
  }

  // Fill in any null parameters with those from the provided defaults
  public void applyDefaults(ConnectParams defaults) {
    username = Optional.ofNullable(username).orElse(defaults.getUsername());
    password = Optional.ofNullable(password).orElse(defaults.getPassword());
    host = Optional.ofNullable(host).orElse(defaults.getHost());
    port = Optional.ofNullable(port).orElse(defaults.getPort());
    dbname = Optional.ofNullable(dbname).orElse(defaults.getDbname());
  }
}
