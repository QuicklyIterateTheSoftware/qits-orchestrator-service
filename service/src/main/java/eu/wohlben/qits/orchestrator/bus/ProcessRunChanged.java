package eu.wohlben.qits.orchestrator.bus;

import eu.wohlben.qits.eventstream.QitsEvent;
import java.time.Instant;
import java.util.UUID;

/**
 * <b>{@code ProcessRunChanged}</b> (qits-2472): a technical process's run opened or closed.
 *
 * <p>Published by {@link RunEventAnnouncer} twice per run — once RUNNING, right after the run's rows
 * were written, and once SUCCEEDED or FAILED, right after the run was closed. A run whose close could
 * not be written gets no second event, because its row still says RUNNING. The fields are the run
 * summary {@code GET /processes/{kind}/runs} serves, minus the summary line: a reader wanting that
 * reads the run.
 *
 * <p>The class lives here, not in a vocabulary jar, for qits-projects' {@code IdeaCreated}'s
 * reason; a consumer decodes it into a record of its own. <b>No field spells an envelope word</b>
 * ({@code name}, {@code signature}, {@code occurredAt}; {@code eventId}'s own accessor aside) — a
 * component named {@code name} or {@code signature} would override {@link QitsEvent}'s method of the
 * same name (qits-2055). {@code changedAt} is the envelope's {@code occurredAt}.
 *
 * @param kind the process kind, {@code gc}
 * @param runId the run, as {@code GET /runs/{id}} takes it
 * @param status RUNNING, SUCCEEDED or FAILED
 * @param trigger {@code manual}, {@code scheduled} or {@code event}
 * @param dryRun whether the run plans instead of deleting
 * @param startedAt when the run opened
 * @param finishedAt when it closed; null while RUNNING
 * @param changedAt when this transition was written
 */
public record ProcessRunChanged(
    UUID eventId,
    String kind,
    UUID runId,
    String status,
    String trigger,
    boolean dryRun,
    Instant startedAt,
    Instant finishedAt,
    Instant changedAt)
    implements QitsEvent {

  public ProcessRunChanged {
    if (eventId == null) {
      eventId = UUID.randomUUID();
    }
  }

  @Override
  public Instant occurredAt() {
    return changedAt;
  }
}
