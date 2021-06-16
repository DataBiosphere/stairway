package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import bio.terra.stairway.Control;
import bio.terra.stairway.FlightStatus;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class FlightCommands {
  private static final Logger logger = LoggerFactory.getLogger(FlightCommands.class);
  private final StairwayService stairwayService;

  @Autowired
  public FlightCommands(StairwayService stairwayService) {
    this.stairwayService = stairwayService;
  }

  // -- Flight commands in alphabetical order --

  // None of the flight command work if we are not connected, so list all commands here
  // so they will not be available.
  @ShellMethodAvailability({
    "count flights",
    "force fatal",
    "force ready",
    "get flight",
    "list dismal",
    "list flights",
    "list owned"
  })
  public Availability availabilityCheck() {
    return stairwayService.isConnected()
        ? Availability.available()
        : Availability.unavailable("you are not connected");
  }

  @ShellMethod(value = "Count flights", key = "count flights")
  public void countFlights(
      @ShellOption(
          value = {"-s", "--status"},
          defaultValue = ShellOption.NULL)
          String status)
      throws Exception {

    try {
      int count = stairwayService.getControl().countFlights(status);
      String context = Optional.ofNullable(status).map(s -> " with status " + s).orElse(StringUtils.EMPTY);
      System.out.println("Found " + count + " flights" + context);
    } catch (Exception ex) {
      Output.error("List flights failed", ex);
    }
  }

  @ShellMethod(value = "Force a flight to FATAL state (dismal failure)", key = "force fatal")
  public void forceFatal(String flightId) throws Exception {
    try {
      Control.Flight flight = stairwayService.getControl().forceFatal(flightId);
      Output.flightSummary(flight);
    } catch (Exception ex) {
      Output.error("Force fatal failed", ex);
    }
  }

  @ShellMethod(value = "Force a flight to the READY state (disown it)", key = "force ready")
  public void forceReady(String flightId) throws Exception {
    try {
      Control.Flight flight = stairwayService.getControl().flightDisown(flightId);
      Output.flightSummary(flight);
    } catch (Exception ex) {
      Output.error("Disown flight failed", ex);
    }
  }

  @ShellMethod(value = "Get one flight", key = "get flight")
  public void getFlight(
      @ShellOption(help = "display log summary", defaultValue = "false") boolean log,
      @ShellOption(help = "display log detail with working map", defaultValue = "false")
          boolean logmap,
      @ShellOption(help = "display input map", defaultValue = "false") boolean input,
      String flightId)
      throws Exception {
    try {
      Control.Flight flight = stairwayService.getControl().getFlight(flightId);
      List<Control.KeyValue> inputKeyValue = null;
      if (input) {
        inputKeyValue = stairwayService.getControl().inputQuery(flightId);
      }
      Output.flightSummary(flight, inputKeyValue);

      if (log || logmap) {
        List<Control.LogEntry> logEntryList = stairwayService.getControl().logQuery(flightId);
        Output.logList(flight, logEntryList, logmap);
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
              defaultValue = ShellOption.NULL)
          String status)
      throws Exception {

    try {
      List<Control.Flight> flightList =
          stairwayService.getControl().listFlightsSimple(offset, limit, status);
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
      List<Control.Flight> flightList = stairwayService.getControl().listOwned(offset, limit);
      Output.flightList(offset, flightList);
    } catch (Exception ex) {
      Output.error("List owned failed", ex);
    }
  }
}
