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
  private final Output output;

  @Autowired
  public ConnectCommands(StairwayService stairwayService, Output output) {
    this.stairwayService = stairwayService;
    this.output = output;
  }

  @ShellMethod(value = "Connect to a Stairway database", key = "connect")
  public void connect(
      @ShellOption(
              value = {"-u", "-U", "--username"},
              defaultValue = ShellOption.NULL)
          String username,
      @ShellOption(
              value = {"-w", "--password"},
              defaultValue = ShellOption.NULL)
          String password,
      @ShellOption(
              value = {"-d", "--dbname"},
              defaultValue = ShellOption.NULL)
          String dbname,
      @ShellOption(
              value = {"-H", "--host"},
              defaultValue = ShellOption.NULL)
          String host,
      @ShellOption(
              value = {"-p", "--port"},
              defaultValue = ShellOption.NULL)
          String port) {

    ConnectParams connectParams = new ConnectParams(username, password, host, port, dbname);

    disconnectIfConnected();
    stairwayService.connectStairway(connectParams);
    output.println(
        "Connected to Stairway on database " + stairwayService.getCurrentConnectParams().dbname());
  }

  @ShellMethod(value = "Disconnect from a Stairway database", key = "disconnect")
  public void disconnect() {
    disconnectIfConnected();
  }

  @ShellMethod(value = "Show the current connection", key = "show connection")
  public void showConnection() {
    if (!stairwayService.isConnected()) {
      output.println("Not connected to a Stairway");
    } else {
      output.showConnection(stairwayService.getCurrentConnectParams());
    }
  }

  private void disconnectIfConnected() {
    if (stairwayService.isConnected()) {
      output.println(
          "Disconnecting from Stairway on database "
              + stairwayService.getCurrentConnectParams().dbname());
      stairwayService.disconnectStairway();
    }
  }
}
