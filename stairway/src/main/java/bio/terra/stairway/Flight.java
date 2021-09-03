package bio.terra.stairway;

import java.util.LinkedList;
import java.util.List;

/**
 * The Flight object is the vehicle Stairway uses for (re)building the steps of a flight in
 * preparation for running the flight. Previously, this class contained the logic for running the
 * flight. That logic is now in the implementation class {@link
 * bio.terra.stairway.impl.FlightRunner}
 *
 * <p>In order for the flight to be re-created on recovery, the construction and configuration have
 * to result in the same set of steps given the same input.
 */
public class Flight {
  private final FlightMap inputParameters;
  private final Object applicationContext;
  private final List<Step> steps;
  private final List<RetryRule> retryRules;

  /**
   * All subclasses must provide a constructor with this signature.
   *
   * @param inputParameters FlightMap of the inputs for the flight
   * @param applicationContext Anonymous context meaningful to the application using Stairway
   */
  public Flight(FlightMap inputParameters, Object applicationContext) {
    this.inputParameters = inputParameters;
    this.applicationContext = applicationContext;
    this.steps = new LinkedList<>();
    this.retryRules = new LinkedList<>();
  }

  // Used by subclasses to build the step list with default no-retry rule
  protected void addStep(Step step) {
    addStep(step, RetryRuleNone.getRetryRuleNone());
  }

  /**
   * Add a step and a retry rule to the respective arrays.
   *
   * @param step subclass of Step
   * @param retryRule subclass of RetryRule
   */
  protected void addStep(Step step, RetryRule retryRule) {
    steps.add(step);
    retryRules.add(retryRule);
  }

  // -- accessors --

  public Object getApplicationContext() {
    return applicationContext;
  }

  public FlightMap getInputParameters() {
    return inputParameters;
  }

  public List<Step> getSteps() {
    return steps;
  }

  public List<RetryRule> getRetryRules() {
    return retryRules;
  }
}
