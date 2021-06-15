package bio.terra.stairctl.commands;

import bio.terra.stairway.Control;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Output {
  private static final Logger logger = LoggerFactory.getLogger(Output.class);

  private static final String FLIGHT_LIST_FORMAT = "%6s %-22s %-30s %-27s %-27s %-12s %-30s";
  private static final String FLIGHT_LIST_DASH =
      "------ ---------------------- ------------------------------ --------------------------- --------------------------- ------------ ------------------------------";
  private static final String STAIRWAY_LIST_FORMAT = "%-40s";
  private static final String STAIRWAY_LIST_DASH = "----------------------------------------";

  public static void flightList(int offset, List<Control.Flight> flightList) {
    if (flightList.isEmpty()) {
      System.out.println("\nNo flights found");
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

  public static void flightSummary(Control.Flight flight) {
    System.out.println("\nflightId  : " + flight.getFlightId());
    System.out.println("class     : " + flight.getClassName());
    System.out.println("submitted : " + flight.getSubmitted().toString());
    System.out.println(
        "completed : " + flight.getCompleted().map(Instant::toString).orElse(StringUtils.EMPTY));
    System.out.println("status    : " + flight.getStatus());
    System.out.println("exception : " + flight.getException().orElse(StringUtils.EMPTY));
    System.out.println("stairwayId: " + flight.getStairwayId().orElse(StringUtils.EMPTY));
  }

  public static void error(String userMessage, Exception ex) {
    System.err.println(": " + ex.getMessage());
    logger.error(userMessage + ": ", ex);
  }

  private static String shortenClassName(String in) {
    if (in.length() <= 30) {
      return in;
    }
    return "..." + StringUtils.right(in, 27);
  }
}
