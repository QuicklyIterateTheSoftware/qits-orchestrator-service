package eu.wohlben.qits.orchestrator.bus;

import eu.wohlben.qits.eventstream.QitsEvent;
import java.time.Instant;
import java.util.UUID;

/**
 * <b>{@code ProcessRunStepChanged}</b> (qits-2472): one step of a run started or ended.
 *
 * <p>Published by {@link RunEventAnnouncer} after each step write returned: RUNNING when the step
 * starts, then its terminal status — SUCCEEDED, FAILED or SKIPPED — when it ends. A step a failure
 * skipped never started, so it gets the terminal event alone. <b>No bodies and no summary</b>: the
 * request and the answer stay on {@code GET /runs/{id}/steps/{stepId}}, which is where a reader that
 * wants them goes.
 *
 * <p>Lives here and names no envelope word, for {@link ProcessRunChanged}'s reasons.
 *
 * @param kind the process kind of the run
 * @param runId the run
 * @param stepId the definition's step id, {@code artifacts.plan}
 * @param status RUNNING, SUCCEEDED, FAILED or SKIPPED
 * @param changedAt when this transition was written
 */
public record ProcessRunStepChanged(
    UUID eventId, String kind, UUID runId, String stepId, String status, Instant changedAt)
    implements QitsEvent {

  public ProcessRunStepChanged {
    if (eventId == null) {
      eventId = UUID.randomUUID();
    }
  }

  @Override
  public Instant occurredAt() {
    return changedAt;
  }
}
