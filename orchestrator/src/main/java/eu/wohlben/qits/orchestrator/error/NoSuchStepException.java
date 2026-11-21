package eu.wohlben.qits.orchestrator.error;

/**
 * The run exists but holds no step of that id — a 404, the same answer as an unknown run (ticket
 * qits-2359). The step id is the definition's ({@code artifacts.plan}), not a storage row's uuid.
 */
public class NoSuchStepException extends OrchestratorException {

  public NoSuchStepException(Object runId, String stepId) {
    super(404, "run '" + runId + "' has no step '" + stepId + "'");
  }
}
