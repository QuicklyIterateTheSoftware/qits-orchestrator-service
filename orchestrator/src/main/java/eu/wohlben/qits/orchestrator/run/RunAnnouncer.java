package eu.wohlben.qits.orchestrator.run;

import java.time.Instant;
import java.util.UUID;

/**
 * Tells the rest of the platform that a run, or one of its steps, changed status (qits-2472), so a
 * page can follow a run without polling {@code GET /runs/{id}}.
 *
 * <p><b>A port, because this module knows of no bus.</b> {@code orchestrator/} does not depend on
 * qits-eventstream — the deployable does — so the announcement leaves through a seam declared here
 * and implemented in {@code service/…/bus/RunEventAnnouncer}. <b>Absent is a supported
 * configuration</b>: {@link RunExecutor} injects it as an {@code Instance<RunAnnouncer>}, and with no
 * implementation a run simply announces nothing. qits-projects' {@code IdeaAnnouncer} is the model.
 *
 * <p><b>Told after the store write returned, never inside it.</b> Every call follows the {@code
 * RunStore} write that made the change true, so an event never describes a row a reader could not
 * yet see — and a write that failed announces nothing: a run whose {@code close} threw gets no
 * terminal event, because its row still says RUNNING.
 *
 * <p><b>An implementation may throw and it changes nothing.</b> The executor catches every {@code
 * RuntimeException} out of either method, logs a WARN and carries on: a throw on the worker would
 * leave a RUNNING row nothing can ever close, and no announcement is worth that. A page that missed
 * an event still has the API, which is the account; the events are only the nudge to read it.
 *
 * <p><b>Status only, never the bodies.</b> A step's request and response stay on {@code GET
 * /runs/{id}/steps/{stepId}}; an event carries what changed and when, and a reader that wants more
 * asks for it.
 */
public interface RunAnnouncer {

  /**
   * A run opened ({@link RunStatus#RUNNING}, once, right after {@code RunStore.open}) or closed
   * ({@link RunStatus#SUCCEEDED} / {@link RunStatus#FAILED}, once, right after {@code
   * RunStore.close}).
   */
  void runChanged(RunChange change);

  /**
   * A step started ({@link RunStatus#RUNNING}, after {@code RunStore.stepRunning}) or ended (its
   * terminal status — SUCCEEDED, FAILED or SKIPPED — after {@code RunStore.stepFinished}). A step a
   * failure skipped never ran, so it gets the terminal call alone.
   */
  void stepChanged(StepChange change);

  /**
   * One run's transition, in the domain's own types; the adapter spells them for the wire.
   *
   * @param kind the process kind, {@code gc}
   * @param runId the run
   * @param status RUNNING on open, SUCCEEDED or FAILED on close
   * @param trigger what started it
   * @param dryRun whether it plans instead of deleting
   * @param startedAt when it opened
   * @param finishedAt when it closed; null while RUNNING
   * @param changedAt when this transition was written
   */
  record RunChange(
      String kind,
      UUID runId,
      RunStatus status,
      RunTrigger trigger,
      boolean dryRun,
      Instant startedAt,
      Instant finishedAt,
      Instant changedAt) {}

  /**
   * One step's transition.
   *
   * @param kind the process kind of the run it belongs to
   * @param runId that run
   * @param stepId the definition's step id, {@code artifacts.plan}
   * @param status RUNNING, or the terminal SUCCEEDED / FAILED / SKIPPED
   * @param changedAt when this transition was written
   */
  record StepChange(String kind, UUID runId, String stepId, RunStatus status, Instant changedAt) {}
}
