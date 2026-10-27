package eu.wohlben.qits.orchestrator.persistence;

import eu.wohlben.qits.db.DbRetry;
import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Sort;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * The only writer of {@code op_run} and {@code op_step}, and the reader the API uses.
 *
 * <p><b>Every method activates a request context</b>, because the caller is usually the executor's
 * own thread and a Hibernate session is bound to that context. A route's call already has one and
 * activating a second is a no-op, so one annotation covers both callers.
 *
 * <p><b>Every write is a {@code DbRetry.inNewTx}.</b> {@code inNewTx} owns the transaction
 * boundary, which is the only way a retry can tell "the body threw, so it certainly never
 * committed" from "the transaction manager reported it" — Narayana spells a lost commit and a real
 * rollback with the same exception. Each write also ends with a flush, which keeps a lost
 * connection on the body's side of that line. This service's writes happen while a deletion is in
 * flight on another host, so losing one is losing the account of what was deleted.
 *
 * <p><b>The steps are written as they transition</b>, not at the end of the run. The UI polls a
 * RUNNING run every two seconds, and a run that wrote its rows at the end would show nine pending
 * cards for minutes and then everything at once.
 */
@ApplicationScoped
public class RunStore implements PanacheRepositoryBase<OpRun, UUID> {

  private static final Logger LOG = Logger.getLogger(RunStore.class);

  /**
   * How long a RUNNING run may stand before {@link #open} treats it as abandoned.
   *
   * <p>A run whose process died (a crash, a full disk, a kill) keeps its RUNNING row forever, and
   * without this every later run of the kind was refused — the gc stopped for good on 2026-10-27
   * (qits-1663). Every step's call has its own timeout, so a live run ends far sooner than this.
   */
  @ConfigProperty(name = "qits.orchestrator.runs.abandon-after")
  Duration abandonAfter;

  /**
   * Opens a run: the run row, plus one PENDING step row per definition, in one transaction.
   *
   * <p><b>The active-run check is inside that transaction</b> rather than a read before it. Two
   * callers pressing the button at once is the ordinary case — a person and the cron — and a check
   * outside the write is a race with a very expensive prize.
   *
   * <p><b>An active run older than {@code abandon-after} does not refuse.</b> Its process is gone,
   * so the run is closed FAILED as abandoned, its unfinished steps with it, and the new run opens.
   */
  @ActivateRequestContext
  public UUID open(
      String kind, RunTrigger trigger, boolean dryRun, List<StepDefinition> steps, Instant now) {
    return DbRetry.inNewTx(
        "open a " + kind + " run",
        () -> {
          OpRun active =
              find("kind = ?1 and status = ?2", kind, RunStatus.RUNNING.name()).firstResult();
          if (active != null) {
            if (!active.startedAt.isBefore(now.minus(abandonAfter))) {
              throw new RunAlreadyActiveException(kind, active.id);
            }
            abandon(active, now);
          }
          OpRun run = new OpRun();
          run.id = UUID.randomUUID();
          run.kind = kind;
          run.trigger = trigger.wireName();
          run.dryRun = dryRun;
          run.status = RunStatus.RUNNING.name();
          run.startedAt = now;
          persist(run);

          int seq = 0;
          for (StepDefinition step : steps) {
            OpStep row = new OpStep();
            row.id = UUID.randomUUID();
            row.runId = run.id;
            row.seq = seq++;
            row.stepId = step.id();
            row.name = step.name();
            row.target = step.target();
            row.dependsOn = step.dependsOn().isEmpty() ? null : String.join(",", step.dependsOn());
            row.status = RunStatus.PENDING.name();
            row.persist();
          }
          getEntityManager().flush();
          return run.id;
        });
  }

  /**
   * Closes a run whose process is gone. Its finish time stays null: nobody knows when, or whether,
   * its last call ended. A step that was RUNNING is FAILED, a PENDING one SKIPPED.
   */
  private void abandon(OpRun run, Instant now) {
    String reason =
        "abandoned: still RUNNING after " + abandonAfter + "; the process that ran it is gone";
    for (OpStep step : OpStep.<OpStep>list("runId", run.id)) {
      if (RunStatus.RUNNING.name().equals(step.status)) {
        step.status = RunStatus.FAILED.name();
        step.error = reason;
      } else if (RunStatus.PENDING.name().equals(step.status)) {
        step.status = RunStatus.SKIPPED.name();
        step.error = reason;
      }
    }
    run.status = RunStatus.FAILED.name();
    run.summary = reason;
    LOG.warnf("The %s run %s started %s was %s.", run.kind, run.id, run.startedAt, reason);
  }

  /** Marks one step RUNNING, so the UI's next poll shows it working. */
  @ActivateRequestContext
  public void stepRunning(UUID runId, String stepId, Instant startedAt) {
    DbRetry.runInNewTx(
        "start step " + stepId,
        () -> {
          OpStep row = step(runId, stepId);
          row.status = RunStatus.RUNNING.name();
          row.startedAt = startedAt;
          getEntityManager().flush();
        });
  }

  /** Writes one step's ending: the verdict, the call, the answer and the human line. */
  @ActivateRequestContext
  public void stepFinished(UUID runId, String stepId, StepResult result, Instant finishedAt) {
    DbRetry.runInNewTx(
        "finish step " + stepId,
        () -> {
          OpStep row = step(runId, stepId);
          row.status = result.status().name();
          row.finishedAt = finishedAt;
          row.summary = result.summary();
          row.error = result.error();
          if (result.call() != null) {
            row.requestMethod = result.call().method();
            row.requestUrl = result.call().url();
            row.requestBody = result.call().body();
          }
          if (result.answer() != null) {
            row.httpStatus = result.answer().httpStatus();
            row.responseBody = result.answer().body();
          }
          getEntityManager().flush();
        });
  }

  /** Closes a run. */
  @ActivateRequestContext
  public void close(UUID runId, RunStatus status, String summary, Instant finishedAt) {
    DbRetry.runInNewTx(
        "close run " + runId,
        () -> {
          OpRun run = findById(runId);
          if (run == null) {
            return;
          }
          run.status = status.name();
          run.summary = summary;
          run.finishedAt = finishedAt;
          getEntityManager().flush();
        });
  }

  /** One run, or empty. */
  @ActivateRequestContext
  public Optional<OpRun> run(UUID id) {
    return Optional.ofNullable(findById(id));
  }

  /** The newest runs of one kind. */
  @ActivateRequestContext
  public List<OpRun> runs(String kind, int limit) {
    return find("kind = ?1", Sort.by("startedAt").descending(), kind).page(0, limit).list();
  }

  /** One run's steps, in the order they ran. */
  @ActivateRequestContext
  public List<OpStep> steps(UUID runId) {
    return OpStep.find("runId = ?1", Sort.by("seq"), runId).list();
  }

  /** The active run of a kind, if there is one. Read-only; {@link #open} is what enforces it. */
  @ActivateRequestContext
  public Optional<OpRun> active(String kind) {
    return Optional.ofNullable(
        find("kind = ?1 and status = ?2", kind, RunStatus.RUNNING.name()).firstResult());
  }

  private static OpStep step(UUID runId, String stepId) {
    OpStep row = OpStep.find("runId = ?1 and stepId = ?2", runId, stepId).firstResult();
    if (row == null) {
      throw new IllegalStateException("run " + runId + " has no step '" + stepId + "'");
    }
    return row;
  }
}
