package eu.wohlben.qits.orchestrator.error;

/**
 * The caller may read this process but not start it — a 403 (ticket qits-2490).
 *
 * <p>Today that is a commissioned agent ({@code qits:agent} alone) asking for a run of a process
 * whose {@code agentStartable()} is false. The sentence names the kind, so the refusal reads as a
 * decision about this process rather than a broken credential.
 */
public class StartNotAllowedException extends OrchestratorException {

  public StartNotAllowedException(String kind) {
    super(
        403,
        "a commissioned agent may not start a "
            + kind
            + " run; only the gc process is open to qits:agent");
  }
}
