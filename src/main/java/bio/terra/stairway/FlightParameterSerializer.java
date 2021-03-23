package bio.terra.stairway;

/**
 * Interface for serializing objects of type T to Strings
 *
 * @param <T> - type to serialize from
 */
public interface FlightParameterSerializer<T> {
  /**
   * Serialize an object of type T, returning a String
   *
   * @param object - the object to be serialized
   */
  public String serialize(T object);
}
