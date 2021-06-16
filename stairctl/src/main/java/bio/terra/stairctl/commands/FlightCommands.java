package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import bio.terra.stairway.Control;
import bio.terra.stairway.FlightStatus;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class FlightCommands {
  private static final Logger logger = LoggerFactory.getLogger(FlightCommands.class);
  private final StairwayService stairwayService;
  private final Control control;

  @Autowired
  public FlightCommands(StairwayService stairwayService) {
    this.stairwayService = stairwayService;
    this.control = stairwayService.getControl();
  }

  // -- Flight commands in alphabetical order --

  @ShellMethod(value = "Force a flight to FATAL state (dismal failure)", key = "force fatal")
  public void forceFatal(String flightId) throws Exception {
    try {
      Control.Flight flight = control.forceFatal(flightId);
      Output.flightSummary(flight);
    } catch (Exception ex) {
      Output.error("Force fatal failed", ex);
    }
  }

  @ShellMethod(value = "Force a flight to the READY state (disown it)", key = "force ready")
  public void forceReady(String flightId) throws Exception {
    try {
      Control.Flight flight = control.flightDisown(flightId);
      Output.flightSummary(flight);
    } catch (Exception ex) {
      Output.error("Disown flight failed", ex);
    }
  }

  @ShellMethod(value = "Get one flight", key = "get flight")
  public void getFlight(
      boolean log,
      boolean input,
      String flightId) throws Exception {
    try {
      Control.Flight flight = control.getFlight(flightId);
      List<Control.KeyValue> inputKeyValue = null;
      if (input) {
        inputKeyValue = control.inputQuery(flightId);
      }
      Output.flightSummary(flight, inputKeyValue);

      if (log) {
        List<Control.LogEntry> logEntryList = control.logQuery(flightId);
        Output.logList( logEntryList);
      }
    } catch (Exception ex) {
      Output.error("Get flight failed", ex);
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
      List<Control.Flight> flightList = control.listFlightsSimple(offset, limit, status);
      Output.flightList(offset, flightList);
    } catch (Exception ex) {
      Output.error("List flights failed", ex);
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
      List<Control.Flight> flightList = control.listOwned(offset, limit);
      Output.flightList(offset, flightList);
    } catch (Exception ex) {
      Output.error("List owned failed", ex);
    }
  }

}
