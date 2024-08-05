package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import bio.terra.stairway.Control;
import bio.terra.stairway.Control.FlightMapEntry;
import bio.terra.stairway.FlightStatus;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class FlightCommands {
  private final StairwayService stairwayService;
  private final Output output;

  @Autowired
  public FlightCommands(StairwayService stairwayService, Output output) {
    this.stairwayService = stairwayService;
    this.output = output;
  }

  // -- Flight commands in alphabetical order --

  // None of the flight command work if we are not connected, so list all commands here,
  // so they will not be available.
  @ShellMethodAvailability({
    "count flights",
    "count owned",
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
          String status) {

    FlightStatus flightStatus = convertToFlightStatus(status);
    try {
      int count = stairwayService.getControl().countFlights(flightStatus);
      String context = Optional.ofNullable(status).map(s -> " with status " + s).orElse("");
      output.println("Found " + count + " flights" + context);
    } catch (Exception ex) {
      output.error("Count flights failed", ex);
    }
  }

  @ShellMethod(value = "Count owned flights", key = "count owned")
  public void countOwned() {

    try {
      int count = stairwayService.getControl().countOwned();
      output.println("Found " + count + " owned flights");
    } catch (Exception ex) {
      output.error("Count flights failed", ex);
    }
  }

  @ShellMethod(value = "Force a flight to FATAL state (dismal failure)", key = "force fatal")
  public void forceFatal(String flightId) {
    try {
      Control.Flight flight = stairwayService.getControl().forceFatal(flightId);
      output.flightSummary(flight);
    } catch (Exception ex) {
      output.error("Force fatal failed", ex);
    }
  }

  @ShellMethod(value = "Force a flight to the READY state (disown it)", key = "force ready")
  public void forceReady(String flightId) {
    try {
      Control.Flight flight = stairwayService.getControl().forceReady(flightId);
      output.flightSummary(flight);
    } catch (Exception ex) {
      output.error("Disown flight failed", ex);
    }
  }

  @ShellMethod(value = "Get one flight", key = "get flight")
  public void getFlight(
      @ShellOption(help = "display log summary", defaultValue = "false") boolean log,
      @ShellOption(help = "display log detail with working map", defaultValue = "false")
          boolean logmap,
      @ShellOption(help = "display input map", defaultValue = "false") boolean input,
      String flightId) {
    try {
      Control.Flight flight = stairwayService.getControl().getFlight(flightId);
      List<FlightMapEntry> inputKeyValue = null;
      if (input) {
        inputKeyValue = stairwayService.getControl().inputQuery(flightId);
      }
      output.flightSummary(flight, inputKeyValue);

      if (log || logmap) {
        List<Control.LogEntry> logEntryList = stairwayService.getControl().logQuery(flightId);
        output.logList(flight, logEntryList, logmap);
      }
    } catch (Exception ex) {
      output.error("Get flight failed", ex);
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
          int limit) {
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
          String status) {

    FlightStatus flightStatus = convertToFlightStatus(status);
    try {
      List<Control.Flight> flightList =
          stairwayService.getControl().listFlightsSimple(offset, limit, flightStatus);
      output.flightList(offset, flightList);
    } catch (Exception ex) {
      output.error("List flights failed", ex);
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
          int limit) {

    try {
      List<Control.Flight> flightList = stairwayService.getControl().listOwned(offset, limit);
      output.flightList(offset, flightList);
    } catch (Exception ex) {
      output.error("List owned failed", ex);
    }
  }

  private FlightStatus convertToFlightStatus(String status) {
    if (status == null) {
      return null;
    }
    try {
      return FlightStatus.valueOf(status);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          "Invalid status value. Values are "
              + Arrays.stream(FlightStatus.values())
                  .map(FlightStatus::toString)
                  .collect(Collectors.joining(", ")));
    }
  }
}
