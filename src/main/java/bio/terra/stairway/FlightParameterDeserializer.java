package bio.terra.stairway;

/**
 * Interface for deserializing objects of type T from Strings
 *
 * @param <T> - type to deserialize to
 */
public interface FlightParameterDeserializer<T> {

  /**
   * Deserialize a String, returning an object of type T
   *
   * @param string - the String to be deserialized
   */
  public T deserialize(String string);
}
