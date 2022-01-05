package bio.terra.stairway;

import java.util.List;

/** Class that holds the return value of the enhanced getFlights Stairway entrypoint */
public class FlightEnumeration {
  private final int totalFlights;
  private final String nextPageToken;
  private final List<FlightState> flightStateList;

  public FlightEnumeration(
      int totalFlights, String nextPageToken, List<FlightState> flightStateList) {
    this.totalFlights = totalFlights;
    this.nextPageToken = nextPageToken;
    this.flightStateList = flightStateList;
  }

  /** @return total number of flights in the filtered set */
  public int getTotalFlights() {
    return totalFlights;
  }

  /** @return encoded string describing the start of the next page */
  public String getNextPageToken() {
    return nextPageToken;
  }

  /** @return list of FlightState in this page */
  public List<FlightState> getFlightStateList() {
    return flightStateList;
  }
}
