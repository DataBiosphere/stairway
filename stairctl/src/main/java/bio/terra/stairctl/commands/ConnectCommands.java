package bio.terra.stairctl.commands;

import bio.terra.stairctl.ConnectParams;
import bio.terra.stairctl.StairwayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class ConnectCommands {
  private final StairwayService stairwayService;

  @Autowired
  public ConnectCommands(StairwayService stairwayService) {
    this.stairwayService = stairwayService;
  }

  @ShellMethod(value = "Connect to a Stairway database", key = "connect")
  public void connect(
      @ShellOption(value = {"-u", "-U", "--username"}, defaultValue = ShellOption.NULL) String username,
      @ShellOption(value = {"-p", "--password"}, defaultValue = ShellOption.NULL) String password,
      @ShellOption(value = {"-d", "--dbname"}, defaultValue = ShellOption.NULL) String dbname,
      @ShellOption(value = {"-h", "--host"}, defaultValue = ShellOption.NULL) String host,
      @ShellOption(value = {"-p", "--port"}, defaultValue = ShellOption.NULL) String port)
      throws Exception {

    ConnectParams connectParams =
        new ConnectParams()
            .username(username)
            .password(password)
            .dbname(dbname)
            .host(host)
            .port(port);

    disconnectIfConnected();
    stairwayService.connectStairway(connectParams);
    System.out.println("Connected to Stairway on database " + connectParams.getDbname());
  }

  @ShellMethod(value = "Disconnect from a Stairway database", key = "disconnect")
  public void disconnect() throws Exception {
    disconnectIfConnected();
  }

  @ShellMethod(value = "Show the current connection", key = "show connection")
  public void showConnection() throws Exception {
    if (!stairwayService.isConnected()) {
      System.out.println("Not connected to a Stairway");
    } else {
      Output.showConnection(stairwayService.getCurrentConnectParams());
    }
  }

  private void disconnectIfConnected() {
    if (stairwayService.isConnected()) {
      System.out.println(
          "Disconnecting from Stairway on database "
              + stairwayService.getCurrentConnectParams().getDbname());
      stairwayService.disconnectStairway();
    }
  }


}
