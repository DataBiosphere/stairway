package bio.terra.stairway;

@FunctionalInterface
public interface QueueProcessFunction<T1, T2, R> {
  R apply(T1 t1, T2 t2) throws InterruptedException;
}
