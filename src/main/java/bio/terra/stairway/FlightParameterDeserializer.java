package bio.terra.stairway;

public abstract class FlightParameterDeserializer<T> {

  protected Class<T> type;

  public FlightParameterDeserializer(Class<T> type) {
    this.type = type;
  }

  public abstract T deserialize(String string);

  public T safeCast(Object object) {
    if (!type.isInstance(object)) {
      throw new ClassCastException("Value is not an instance of type " + type.getName());
    }
    return type.cast(object);
  }
}
