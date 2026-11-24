package eu.wohlben.qits.orchestrator.error;

/**
 * A dry run asked of a process that has none — a 400 (ticket qits-2490).
 *
 * <p>Refused rather than quietly started as a real run: the caller asked for a run that changes
 * nothing, and the peer door this process calls has no such mode. Starting it anyway would do the
 * real work under a label that says it did not.
 */
public class DryRunNotOfferedException extends OrchestratorException {

  public DryRunNotOfferedException(String kind) {
    super(
        400,
        "the "
            + kind
            + " process has no dry run: the peer door it calls has no dry-run mode, so a run of it"
            + " always does the real work; start it with {\"dryRun\":false}");
  }
}
