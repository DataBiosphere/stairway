package bio.terra.stairctl.commands;

import bio.terra.stairctl.ConnectParams;
import bio.terra.stairway.Control;
import bio.terra.stairway.Control.FlightMapEntry;
import bio.terra.stairway.Control.LogEntry;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;
import org.springframework.stereotype.Component;

@Component
public class Output {

  private final Terminal terminal;

  private static final int CLASS_DISPLAY_LENGTH = 36;
  private static final String FLIGHT_LIST_FORMAT =
      "%6s %-36s %-" + CLASS_DISPLAY_LENGTH + "s %-27s %-27s %-12s %-30s\n";
  private static final String FLIGHT_LIST_DASH =
      "------ ------------------------------------ ------------------------------------ --------------------------- --------------------------- ------------ ------------------------------";
  private static final String STAIRWAY_LIST_FORMAT = "%-40s";
  private static final String STAIRWAY_LIST_DASH = "----------------------------------------";
  private static final String LOG_LIST_FORMAT = "%s%6s %-9s %-27s %13s %-5s %s\n";
  private static final String LOG_LIST_DASH =
      "%s------ --------- --------------------------- ------------- ----- --------------------------------------------------\n";

  private record Pair(String key, Object value) {
    static Pair of(String key, Object value) {
      return new Pair(key, value);
    }
  }

  public Output(Terminal terminal) {
    this.terminal = terminal;
  }

  public void println(String messsage) {
    terminal.writer().println(messsage);
  }

  public void println() {
    terminal.writer().println();
  }

  public void printf(String format, Object... args) {
    terminal.writer().printf(format, args);
  }

  public void flightList(int offset, List<Control.Flight> flightList) {
    if (flightList.isEmpty()) {
      println("\nNo flights found");
    } else {
      int counter = offset;
      printf(
          FLIGHT_LIST_FORMAT,
          "\nOffset",
          "FlightId",
          "Class",
          "Submitted",
          "Completed",
          "Status",
          "StairwayId");
      println(FLIGHT_LIST_DASH);
      for (Control.Flight flight : flightList) {
        printf(
            FLIGHT_LIST_FORMAT,
            counter++,
            flight.getFlightId(),
            shortenClassName(flight.getClassName()),
            flight.getSubmitted(),
            flight.getCompleted().map(Instant::toString).orElse(""),
            flight.getStatus(),
            flight.getStairwayId().orElse(""));
      }
      println();
    }
  }

  public void logList(
      Control.Flight flight, List<Control.LogEntry> logEntryList, boolean showDetail) {
    final String initialIndent = "  ";
    final String entryIndent = initialIndent + "  ";
    final String mapIndent = entryIndent + "  ";

    printf("%sflight log:%n", initialIndent);
    int lastIndex = logEntryList.size() - 1;

    // Summary format has a header
    if (!showDetail) {
      printf(
          LOG_LIST_FORMAT,
          entryIndent,
          "Step",
          "Direction",
          "Log Time",
          "Duration",
          "Rerun",
          "Exception");
      printf(LOG_LIST_DASH, entryIndent);
    }

    for (int i = 0; i <= lastIndex; i++) {
      Instant endInstant =
          (i == lastIndex)
              ? flight.getCompleted().orElse(null)
              : logEntryList.get(i + 1).getLogTime();
      LogEntry logEntry = logEntryList.get(i);
      String durationString = formatDuration(logEntry.getLogTime(), endInstant);
      if (showDetail) {
        printf("%sLog Entry:%n", entryIndent);
        displayLogEntry(mapIndent, logEntry, durationString);
      } else {
        printf(
            LOG_LIST_FORMAT,
            entryIndent,
            logEntry.getStepIndex(),
            logEntry.getDirection(),
            logEntry.getLogTime(),
            durationString,
            logEntry.isRerun(),
            logEntry.getException().orElse(""));
      }
    }
  }

  private static String formatDuration(Instant startInstant, @Nullable Instant endInstant) {
    if (endInstant == null) {
      return "incomplete";
    }
    Duration diff = Duration.between(startInstant, endInstant);
    return String.format(
        "%d:%02d:%02d.%03d",
        diff.toHours(), diff.toMinutesPart(), diff.toSecondsPart(), diff.toMillisPart());
  }

  private void displayLogEntry(String indent, Control.LogEntry logEntry, String durationString) {
    List<Pair> pairList =
        List.of(
            Pair.of("stepIndex", logEntry.getStepIndex()),
            Pair.of("direction", logEntry.getDirection()),
            Pair.of("logTime", logEntry.getLogTime()),
            Pair.of("duration", durationString),
            Pair.of("rerun", logEntry.isRerun()),
            Pair.of("exception", logEntry.getException().orElse(null)));

    int maxLength = display(indent, pairList);
    String mapTitle = "%s%-" + maxLength + "s:%n";
    printf(mapTitle, indent, "workingMap");
    keyValue(indent + "  ", logEntry.getWorkingMap());
  }

  public void keyValue(String indent, List<FlightMapEntry> keyValueList) {
    keyValueList.sort(FlightMapEntry::compareTo);
    List<Pair> pairList =
        keyValueList.stream().map(e -> new Pair(e.getKey(), e.getValue())).toList();
    display(indent, pairList);
  }

  public void stairwayList(List<String> stairwayList) {
    if (stairwayList.isEmpty()) {
      println("\nNo stairway instances found");
    } else {
      printf(STAIRWAY_LIST_FORMAT, "\nStairwayId");
      println(STAIRWAY_LIST_DASH);
      for (String id : stairwayList) {
        printf(STAIRWAY_LIST_FORMAT, id);
      }
      println();
    }
  }

  // Simple case with no input map
  public void flightSummary(Control.Flight flight) {
    flightSummary(flight, null);
  }

  public void flightSummary(Control.Flight flight, List<FlightMapEntry> inputMap) {
    final String indent = "  ";

    List<Pair> pairList =
        List.of(
            Pair.of("class", flight.getClassName()),
            Pair.of("submitted", flight.getSubmitted()),
            Pair.of("completed", flight.getCompleted().orElse(null)),
            Pair.of(
                "duration",
                formatDuration(flight.getSubmitted(), flight.getCompleted().orElse(null))),
            Pair.of("status", flight.getStatus()),
            Pair.of("exception", flight.getException().orElse(null)),
            Pair.of("stairwayId", flight.getStairwayId().orElse(null)));

    printf("%nFlight: %s%n", flight.getFlightId());
    int maxLength = display(indent, pairList);
    if (inputMap != null) {
      String inputTitle = "%s%-" + maxLength + "s:%n";
      printf(inputTitle, indent, "inputMap");
      keyValue(indent + "  ", inputMap);
    }
  }

  public void showConnection(ConnectParams connectParams) {
    println("Stairway Connection:");
    display(
        "  ",
        List.of(
            Pair.of("dbname", connectParams.dbname()),
            Pair.of("username", connectParams.username()),
            Pair.of("host", connectParams.host()),
            Pair.of("port", connectParams.port())));
  }

  /**
   * This method makes an aligned display of pairs of names and values. It computes the longest name
   * and uses that to generate a String format for presenting the data.
   *
   * @param indent string to prefix each line with
   * @param pairList pairs of field names and field data
   * @return max name length, to allow the caller to align with what we displayed
   */
  private int display(String indent, List<Pair> pairList) {
    // Compute the max length of the first string
    OptionalInt longest = pairList.stream().map(Pair::key).mapToInt(String::length).max();
    if (longest.isEmpty()) {
      return 0;
    }
    int maxLength = longest.getAsInt();
    String displayFormat = "%s%-" + maxLength + "s: %s\n";
    for (Pair pair : pairList) {
      printf(displayFormat, indent, pair.key, pair.value != null ? pair.value : "");
    }
    return maxLength;
  }

  public void error(String userMessage, Exception ex) {
    println(userMessage + ": " + ex.getMessage());
  }

  private static String shortenClassName(String in) {
    String shortName = ClassUtils.getAbbreviatedName(in, CLASS_DISPLAY_LENGTH);
    if (shortName.length() > CLASS_DISPLAY_LENGTH) {
      return ".." + StringUtils.right(in, CLASS_DISPLAY_LENGTH - 2);
    }
    return shortName;
  }
}
