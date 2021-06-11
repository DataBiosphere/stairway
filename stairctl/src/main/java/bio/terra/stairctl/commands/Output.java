package bio.terra.stairctl.commands;

import bio.terra.stairway.FlightState;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.List;

public class Output {
  private static final String FLIGHT_LIST_FORMAT = "%6s %-22s %-27s %-27s %-12s %-5s %-30s";
  private static final String FLIGHT_LIST_DASH =
      "------ ---------------------- --------------------------- --------------------------- ------------ ----- ------------------------------";
  private static final String STAIRWAY_LIST_FORMAT = "%-40s";
  private static final String STAIRWAY_LIST_DASH = "----------------------------------------";

  public static void flightStateList(int offset, List<FlightState> flightList) {
    if (flightList.isEmpty()) {
      System.out.println("\nNo flights found");
    } else {
      int counter = offset;
      System.out.println(
          String.format(
              FLIGHT_LIST_FORMAT,
              "\nOffset",
              "FlightId",
              "Submitted",
              "Completed",
              "Status",
              "Active",
              "StairwayId"));
      System.out.println(FLIGHT_LIST_DASH);
      for (FlightState flight : flightList) {
        System.out.println(
            String.format(
                FLIGHT_LIST_FORMAT,
                counter++,
                flight.getFlightId(),
                flight.getSubmitted().toString(),
                flight.getCompleted().map(Instant::toString).orElse(StringUtils.EMPTY),
                flight.getFlightStatus(),
                flight.isActive(),
                fixNull(flight.getStairwayId())));
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

  public static void flightState(FlightState flight) {
    System.out.println("\nflightId  : " + flight.getFlightId());
    System.out.println("submitted : " + flight.getSubmitted().toString());
    System.out.println(
        "completed : " + flight.getCompleted().map(Instant::toString).orElse(StringUtils.EMPTY));
    System.out.println("status    : " + flight.getFlightStatus());
    System.out.println("active    : " + flight.isActive());
    System.out.println("stairwayId: " + fixNull(flight.getStairwayId()));
  }

  private static String fixNull(String in) {
    if (in == null) {
      return StringUtils.EMPTY;
    }
    return in;
  }
}
