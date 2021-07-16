package bio.terra.stairway.impl;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightSupport;
import bio.terra.stairway.exception.MakeFlightException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightFactory {
  private static final Logger logger = LoggerFactory.getLogger(FlightFactory.class);

  public Flight makeFlight(
      Class<? extends Flight> flightClass,
      FlightMap inputParameters,
      Object context,
      FlightDebugInfo debugInfo,
      FlightSupport flightSupport) {
    try {
      // Find the flightClass constructor that takes the input parameter map and
      // use it to make the flight.
      Constructor constructor = flightClass.getConstructor(FlightMap.class, Object.class);
      Flight flight = (Flight) constructor.newInstance(inputParameters, context);
      flight.setDebugInfo(debugInfo);
      flight.setFlightSupport(flightSupport);
      return flight;
    } catch (InvocationTargetException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException ex) {
      throw new MakeFlightException("Failed to make a flight from class '" + flightClass + "'", ex);
    }
  }

  public Flight makeFlightFromName(
      String className,
      FlightMap inputMap,
      Object context,
      FlightDebugInfo debugInfo,
      FlightSupport flightSupport) {
    try {
      Class<?> someClass = Class.forName(className);
      if (Flight.class.isAssignableFrom(someClass)) {
        Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
        return makeFlight(flightClass, inputMap, context, debugInfo, flightSupport);
      }
      // Error case
      throw new MakeFlightException(
          "Failed to make a flight from class name '"
              + className
              + "' - it is not a subclass of Flight");

    } catch (ClassNotFoundException ex) {
      throw new MakeFlightException(
          "Failed to make a flight from class name '" + className + "'", ex);
    }
  }
}
