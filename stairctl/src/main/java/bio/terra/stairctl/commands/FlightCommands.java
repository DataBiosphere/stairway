package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class FlightCommands {
  private final StairwayService stairwayService;

  @Autowired
  public FlightCommands(StairwayService stairwayService) {
    this.stairwayService = stairwayService;
  }

  // -- Flight commands in alphabetical order --

  @ShellMethod(value = "Force a flight to FATAL state (dismal failure)", key = "force fatal")
  public void forceFatal(String flightId) throws Exception {
    try {
      FlightState flight = stairwayService.get().forceFatal(flightId);
      Output.flightState(flight);
    } catch (Exception ex) {
      System.err.println("Force fatal failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  @ShellMethod(value = "Force a flight to the READY state (disown it)", key = "force ready")
  public void forceReady(String flightId) throws Exception {
    try {
      FlightState flight = stairwayService.get().disownFlight(flightId);
      Output.flightState(flight);
    } catch (Exception ex) {
      System.err.println("Disown flight failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  @ShellMethod(value = "Get one flight", key = "get flight")
  public void getFlight(String flightId) throws Exception {
    try {
      FlightState flight = stairwayService.get().getFlight(flightId);
      Output.flightState(flight);
    } catch (Exception ex) {
      System.err.println("Get flight failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  @ShellMethod(value = "List dismal failure flights", key = "list dismal")
  public void listDismal(
      @ShellOption(
              value = {"-o", "--offset"},
              defaultValue = "0")
          int offset,
      @ShellOption(
              value = {"-l", "--limit"},
              defaultValue = "20")
          int limit)
      throws Exception {
    listFlights(offset, limit, FlightStatus.FATAL.toString());
  }

  @ShellMethod(value = "List flights", key = "list flights")
  public void listFlights(
      @ShellOption(
              value = {"-o", "--offset"},
              defaultValue = "0")
          int offset,
      @ShellOption(
              value = {"-l", "--limit"},
              defaultValue = "20")
          int limit,
      @ShellOption(
              value = {"-s", "--status"},
              defaultValue = "__NULL__")
          String status)
      throws Exception {

    try {
      List<FlightState> flightList = stairwayService.get().listFlights(offset, limit, status);
      Output.flightStateList(offset, flightList);
    } catch (Exception ex) {
      System.err.println("List flights failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  @ShellMethod(value = "List owned flights", key = "list owned")
  public void listOwnedFlights(
      @ShellOption(
              value = {"-o", "--offset"},
              defaultValue = "0")
          int offset,
      @ShellOption(
              value = {"-l", "--limit"},
              defaultValue = "20")
          int limit)
      throws Exception {

    try {
      List<FlightState> flightList = stairwayService.get().listOwned(offset, limit);
      Output.flightStateList(offset, flightList);
    } catch (Exception ex) {
      System.err.println("List owned failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }
}
