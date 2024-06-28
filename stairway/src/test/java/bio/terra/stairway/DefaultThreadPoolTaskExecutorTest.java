package bio.terra.stairway;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("unit")
class DefaultThreadPoolTaskExecutorTest {

  private static Stream<Arguments> defaultThreadPoolTaskExecutor() {
    return Stream.of(
        Arguments.of(null, DefaultThreadPoolTaskExecutor.DEFAULT_MAX_PARALLEL_FLIGHTS),
        Arguments.of(-1, DefaultThreadPoolTaskExecutor.DEFAULT_MAX_PARALLEL_FLIGHTS),
        Arguments.of(0, DefaultThreadPoolTaskExecutor.DEFAULT_MAX_PARALLEL_FLIGHTS),
        Arguments.of(1, 1),
        Arguments.of(50, 50));
  }

  @ParameterizedTest
  @MethodSource
  void defaultThreadPoolTaskExecutor(Integer maxParallelFlights, Integer expectedPoolSize) {
    var executor = new DefaultThreadPoolTaskExecutor(maxParallelFlights);
    assertThat(executor.getCorePoolSize(), equalTo(expectedPoolSize));
    assertThat(executor.getMaxPoolSize(), equalTo(expectedPoolSize));
    assertThat(executor.getKeepAliveSeconds(), equalTo(0));
    assertThat(executor.getThreadNamePrefix(), equalTo("stairway-thread-"));
    assertTrue(executor.isRunning());
  }
}
