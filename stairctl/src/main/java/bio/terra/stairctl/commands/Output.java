package bio.terra.stairctl.commands;

import bio.terra.stairctl.ConnectParams;
import bio.terra.stairway.Control;
import bio.terra.stairway.Control.FlightMapEntry;
import bio.terra.stairway.Control.LogEntry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Output {
  private static final Logger logger = LoggerFactory.getLogger(Output.class);

  private static final int CLASS_DISPLAY_LENGTH = 36;
  private static final String FLIGHT_LIST_FORMAT =
      "%6s %-36s %-" + CLASS_DISPLAY_LENGTH + "s %-27s %-27s %-12s %-30s";
  private static final String FLIGHT_LIST_DASH =
      "------ ------------------------------------ ------------------------------------ --------------------------- --------------------------- ------------ ------------------------------";
  private static final String STAIRWAY_LIST_FORMAT = "%-40s";
  private static final String STAIRWAY_LIST_DASH = "----------------------------------------";
  private static final String LOG_LIST_FORMAT = "%s%6s %-9s %-27s %13s %-5s %s";
  private static final String LOG_LIST_DASH =
      "%s------ --------- --------------------------- ------------- ----- --------------------------------------------------";

  public static void flightList(int offset, List<Control.Flight> flightList) {
    if (flightList.isEmpty()) {
      System.out.println("%nNo flights found");
    } else {
      int counter = offset;
      System.out.println(
          String.format(
              FLIGHT_LIST_FORMAT,
              "\nOffset",
              "FlightId",
              "Class",
              "Submitted",
              "Completed",
              "Status",
              "StairwayId"));
      System.out.println(FLIGHT_LIST_DASH);
      for (Control.Flight flight : flightList) {
        System.out.println(
            String.format(
                FLIGHT_LIST_FORMAT,
                counter++,
                flight.getFlightId(),
                shortenClassName(flight.getClassName()),
                flight.getSubmitted().toString(),
                flight.getCompleted().map(Instant::toString).orElse(StringUtils.EMPTY),
                flight.getStatus(),
                flight.getStairwayId().orElse(StringUtils.EMPTY)));
      }
      System.out.println();
    }
  }

  public static void logList(
      Control.Flight flight, List<Control.LogEntry> logEntryList, boolean showDetail) {
    final String initialIndent = "  ";
    final String entryIndent = initialIndent + "  ";
    final String mapIndent = entryIndent + "  ";

    System.out.println(String.format("%sflight log:", initialIndent));
    int lastIndex = logEntryList.size() - 1;

    // Summary format has a header
    if (!showDetail) {
      System.out.println(
          String.format(
              LOG_LIST_FORMAT,
              entryIndent,
              "Step",
              "Direction",
              "Log Time",
              "Duration",
              "Rerun",
              "Exception"));
      System.out.println(String.format(LOG_LIST_DASH, entryIndent));
    }

    for (int i = 0; i <= lastIndex; i++) {
      Instant endInstant =
          (i == lastIndex) ? flight.getCompleted().orElse(null) : logEntryList.get(i + 1).getLogTime();
      LogEntry logEntry = logEntryList.get(i);
      String durationString = formatDuration(logEntry.getLogTime(), endInstant);
      if (showDetail) {
        System.out.println(String.format("%sLog Entry:", entryIndent));
        displayLogEntry(mapIndent, logEntry, durationString);
      } else {
        System.out.println(
            String.format(
                LOG_LIST_FORMAT,
                entryIndent,
                logEntry.getStepIndex(),
                logEntry.getDirection().toString(),
                logEntry.getLogTime().toString(),
                durationString,
                logEntry.isRerun(),
                logEntry.getException().orElse(StringUtils.EMPTY)));
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

  private static void displayLogEntry(
      String indent, Control.LogEntry logEntry, String durationString) {
    List<ImmutablePair<String, String>> pairList = new ArrayList<>();
    pairList.add(new ImmutablePair<>("stepIndex", Integer.toString(logEntry.getStepIndex())));
    pairList.add(new ImmutablePair<>("direction", logEntry.getDirection().toString()));
    pairList.add(new ImmutablePair<>("logTime", logEntry.getLogTime().toString()));
    pairList.add(new ImmutablePair<>("duration", durationString));
    pairList.add(new ImmutablePair<>("rerun", Boolean.toString(logEntry.isRerun())));
    pairList.add(
        new ImmutablePair<>("exception", logEntry.getException().orElse(StringUtils.EMPTY)));
    int maxLength = display(indent, pairList);
    String mapTitle = "%s%-" + maxLength + "s:";
    System.out.println(String.format(mapTitle, indent, "workingMap"));
    keyValue(indent + "  ", logEntry.getWorkingMap());
  }

  public static void keyValue(String indent, List<FlightMapEntry> keyValueList) {
    keyValueList.sort(FlightMapEntry::compareTo);
    List<ImmutablePair<String, String>> pairList = new ArrayList<>();
    for (FlightMapEntry keyValue : keyValueList) {
      pairList.add(new ImmutablePair<>(keyValue.getKey(), keyValue.getValue()));
    }
    display(indent, pairList);
  }

  public static void stairwayList(List<String> stairwayList) {
    if (stairwayList.isEmpty()) {
      System.out.println("\nNo stairway instances found");
    } else {
      System.out.println(String.format(STAIRWAY_LIST_FORMAT, "\nStairwayId"));
      System.out.println(STAIRWAY_LIST_DASH);
      for (String id : stairwayList) {
        System.out.println(String.format(STAIRWAY_LIST_FORMAT, id));
      }
      System.out.println();
    }
  }

  // Simple case with no input map
  public static void flightSummary(Control.Flight flight) {
    flightSummary(flight, null);
  }

  public static void flightSummary(Control.Flight flight, List<FlightMapEntry> inputMap) {
    final String indent = "  ";

    List<ImmutablePair<String, String>> pairList = new ArrayList<>();
    pairList.add(new ImmutablePair<>("class", flight.getClassName()));
    pairList.add(new ImmutablePair<>("submitted", flight.getSubmitted().toString()));
    pairList.add(
        new ImmutablePair<>(
            "completed", flight.getCompleted().map(Instant::toString).orElse(StringUtils.EMPTY)));
    pairList.add(
        new ImmutablePair<>(
            "duration", formatDuration(flight.getSubmitted(), flight.getCompleted().orElse(null))));
    pairList.add(new ImmutablePair<>("status", flight.getStatus().toString()));
    pairList.add(new ImmutablePair<>("exception", flight.getException().orElse(StringUtils.EMPTY)));
    pairList.add(
        new ImmutablePair<>("stairwayId", flight.getStairwayId().orElse(StringUtils.EMPTY)));

    System.out.println(String.format("%nFlight: %s", flight.getFlightId()));
    int maxLength = display(indent, pairList);
    if (inputMap != null) {
      String inputTitle = "%s%-" + maxLength + "s:";
      System.out.println(String.format(inputTitle, indent, "inputMap"));
      keyValue(indent + "  ", inputMap);
    }
  }

  public static void showConnection(ConnectParams connectParams) {
    List<ImmutablePair<String, String>> pairList = new ArrayList<>();
    pairList.add(new ImmutablePair<>("dbname", connectParams.getDbname()));
    pairList.add(new ImmutablePair<>("username", connectParams.getUsername()));
    pairList.add(new ImmutablePair<>("host", connectParams.getHost()));
    pairList.add(new ImmutablePair<>("port", connectParams.getPort()));
    System.out.println("Stairway Connection:");
    display("  ", pairList);
  }

  /**
   * This method makes an aligned display of pairs of names and values. It computes the longest name
   * and uses that to generate a String format for presenting the data.
   *
   * @param indent string to prefix each line with
   * @param pairList pairs of field names and field data
   * @return max name length, to allow the caller to align with what we displayed
   */
  private static int display(String indent, List<ImmutablePair<String, String>> pairList) {
    // Compute the max length of the first string
    Optional<String> longest =
        pairList.stream().map(Pair::getLeft).max(Comparator.comparing(String::length));
    if (longest.isEmpty()) {
      return 0;
    }
    int maxLength = longest.get().length();
    String displayFormat = "%s%-" + maxLength + "s: %s";
    for (ImmutablePair<String, String> pair : pairList) {
      System.out.println(String.format(displayFormat, indent, pair.getLeft(), pair.getRight()));
    }
    return maxLength;
  }

  public static void error(String userMessage, Exception ex) {
    System.err.println(": " + ex.getMessage());
    logger.error(userMessage + ": ", ex);
  }

  private static String shortenClassName(String in) {
    String shortName = ClassUtils.getAbbreviatedName(in, CLASS_DISPLAY_LENGTH);
    if (shortName.length() > CLASS_DISPLAY_LENGTH) {
      return ".." + StringUtils.right(in, CLASS_DISPLAY_LENGTH - 2);
    }
    return shortName;
  }
}
