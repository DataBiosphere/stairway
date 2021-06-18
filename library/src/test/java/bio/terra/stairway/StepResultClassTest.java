package bio.terra.stairway;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class StepResultClassTest {
  private static String bad = "bad bad bad";

  @Test
  public void testStepResultSuccess() {
    StepResult result = StepResult.getStepResultSuccess();
    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_SUCCESS));
    Optional<Exception> exception = result.getException();
    assertFalse(exception.isPresent());
  }

  @Test
  public void testStepResultError() {
    StepResult result =
        new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new IllegalArgumentException(bad));
    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> exception = result.getException();
    assertTrue(exception.isPresent());
    assertTrue(exception.get() instanceof IllegalArgumentException);
    assertThat(exception.get().getMessage(), is(bad));
  }
}
