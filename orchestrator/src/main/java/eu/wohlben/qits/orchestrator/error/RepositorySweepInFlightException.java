package eu.wohlben.qits.orchestrator.error;

/**
 * A gc run was asked to start while an artifacts cleanup sweep is in flight — a 409 (ticket
 * qits-2138).
 *
 * <p>The mirror of {@link RunAlreadyActiveException}, and the same safety property seen from the
 * other side: a repository sweep and a gc run both hand qits-artifacts a pin set read at their own
 * moment and ask it to delete, so the two must never overlap. The message names the repository
 * being swept, so the caller knows what it is waiting for.
 */
public class RepositorySweepInFlightException extends OrchestratorException {

  private final String repository;

  public RepositorySweepInFlightException(String repository) {
    super(409, "a repository sweep is in flight: " + repository);
    this.repository = repository;
  }

  public String repository() {
    return repository;
  }
}
