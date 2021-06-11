package bio.terra.stairway;

/**
 * Stairway Flight objects need to be created by class and by name. In vanilla Java configurations,
 * Stairway's flight factory can be used. In other configurations, such as Spring, Stairway is not
 * able to properly construct objects with all of the autowiring set up. In those cases, the
 * application can provide an implementation of FlightFactory when constructing Stairway.
 */
public interface FlightFactory {
  /**
   * Create a flight object with the provided input parameters and application context.
   *
   * @param flightClass class of the flight to create
   * @param inputParameters input FlightMap for the constructor - as supplied on the submit call
   * @param context application context for the flight - as supplied to the Stairway builder
   * @param debugInfo optional debug information for flight testing
   * @return Subclass of the Flight class
   */
  Flight makeFlight(
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      Object context,
      FlightDebugInfo debugInfo);

  /**
   * Create a flight object given the class name of the flight.
   *
   * @param className string name of the class to construct
   * @param inputMap input FlightMap for the constructor
   * @param context application context for the flight - as supplied to the Stairway builder
   * @param debugInfo optional debug information for flight testing
   * @return Subclass of the Flight class
   */
  Flight makeFlightFromName(
      String className, FlightMap inputMap, Object context, FlightDebugInfo debugInfo);
}
