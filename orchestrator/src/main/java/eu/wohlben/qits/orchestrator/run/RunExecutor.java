package eu.wohlben.qits.orchestrator.run;

import eu.wohlben.qits.orchestrator.error.DryRunNotOfferedException;
import eu.wohlben.qits.orchestrator.error.NoSuchProcessException;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.ProcessRegistry;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.SkipKind;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * Runs one technical process, start to finish, on a thread of its own.
 *
 * <p><b>ONE THREAD FOR THE WHOLE SERVICE.</b> Not one per kind: a run's work is other people's
 * disks, and two runs overlapping would be two plans against two moments of the same stores. One
 * thread also makes the run log a sequence rather than an interleaving, which is what a person
 * reading it expects. A second run of the SAME kind is refused earlier, by {@link RunStore#open}.
 *
 * <p><b>The route does not wait.</b> {@code POST /processes/{kind}/runs} answers 202 with the id as
 * soon as the rows exist; a gc run is minutes of somebody else's pruning and an HTTP request is the
 * wrong place to hold it. The client polls {@code GET /runs/{id}} today; a page can instead follow
 * the run on the event bus — {@link RunAnnouncer} — and read the run when an event says it moved.
 *
 * <h2>What it announces</h2>
 *
 * The run opening (RUNNING) and closing (SUCCEEDED / FAILED), and each step starting and ending, in
 * the order they happened, each <b>after</b> the {@link RunStore} write that made it true returned
 * — never inside it, and never for a write that threw. Every announcement is wrapped so that it
 * cannot throw: an announcer that blows up is a WARN, and the run carries on and closes exactly as
 * it would have. {@link #execute} never throws, and an event is not worth breaking that for.
 *
 * <h2>What a failure does</h2>
 *
 * Steps run in DECLARATION ORDER. Before each one, its dependencies are looked at:
 *
 * <ul>
 *   <li>none of them FAILED — the step runs;
 *   <li>one FAILED, or was itself skipped BECAUSE something failed — the step is SKIPPED with
 *       {@code error = "skipped: <step> failed"}, naming the step that actually FAILED rather than
 *       the skipped one in between. A cascade that named its neighbour would make a reader walk the
 *       graph backwards to find the cause.
 * </ul>
 *
 * <b>A POLICY skip is not a failure and does not cascade.</b> {@code artifacts.sweep} is skipped on
 * a dry run because that is what a dry run is, so {@code usage.after} still runs — see {@link
 * SkipKind}, and the live run that proved the distinction has to be a field rather than a
 * convention.
 *
 * <b>Independent steps still run.</b> A failure skips only the steps whose edges reach it. Which
 * steps those are is the process's call: the gc process makes every deleter depend on every pin
 * read, so a failed pin read stops every delete in the run and leaves the reads alone.
 *
 * <p><b>The run is FAILED if any step FAILED.</b> A SKIPPED step does not fail it: a skip is the
 * consequence of a failure already counted.
 *
 * <p><b>A FAILED run is a WARN, naming every step that failed</b> (ticket qits-1733): its id, its
 * peer, its error and the start of what the peer said. At INFO, twelve gc runs in one week deleted
 * nothing over one transient 503 and nobody noticed; a SUCCEEDED run stays INFO.
 */
@ApplicationScoped
public class RunExecutor {

  private static final Logger LOG = Logger.getLogger(RunExecutor.class);

  @Inject ProcessRegistry registry;

  @Inject RunStore runs;

  @Inject PeerClient peers;

  /**
   * Where the run's transitions are told, if anywhere: {@code service/}'s bus adapter in a
   * deployment, a recorder in this module's suite, and nothing at all when unresolvable — see
   * {@link #announce}.
   */
  @Inject Instance<RunAnnouncer> announcer;

  /**
   * The one worker. A daemon thread with a name, so a stuck run is identifiable in a thread dump
   * and a JVM shutting down is not held open by a prune somebody else is still doing.
   */
  private final ExecutorService worker =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "qits-orchestrator-run");
            thread.setDaemon(true);
            return thread;
          });

  void onShutdown(@Observes ShutdownEvent event) {
    worker.shutdown();
    try {
      // Long enough for a step in flight to record its answer, short enough that a redeploy is not
      // held up by a peer that will never reply — its call has its own timeout anyway.
      if (!worker.awaitTermination(10, TimeUnit.SECONDS)) {
        worker.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      worker.shutdownNow();
    }
  }

  /**
   * Opens a run and queues it.
   *
   * @return the run id, which the route answers with a 202
   * @throws NoSuchProcessException the kind is not registered — a 404
   * @throws DryRunNotOfferedException a dry run of a process that has none — 400, before any row
   *     is written
   * @throws eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException one is already going — 409
   */
  public UUID start(String kind, RunTrigger trigger, boolean dryRun) {
    TechnicalProcess process =
        registry.byKind(kind).orElseThrow(() -> new NoSuchProcessException(kind));
    if (dryRun && !process.dryRunnable()) {
      throw new DryRunNotOfferedException(kind);
    }
    Instant startedAt = Instant.now();
    UUID runId = runs.open(kind, trigger, dryRun, process.steps(), startedAt);
    // BEFORE the worker is handed the run, so the RUNNING event is certainly ahead of every step's.
    RunAnnouncer.RunChange opened =
        new RunAnnouncer.RunChange(
            kind, runId, RunStatus.RUNNING, trigger, dryRun, startedAt, null, startedAt);
    announce("the opening of " + kind + " run " + runId, to -> to.runChanged(opened));
    worker.execute(() -> execute(process, runId, trigger, dryRun, startedAt));
    return runId;
  }

  /**
   * Runs one process to the end. It never throws: a run that blew up is a FAILED run with the
   * sentence in its summary, because a thrown exception on the worker would leave a RUNNING row
   * nothing can ever close and no second run of that kind would be allowed again.
   */
  void execute(
      TechnicalProcess process, UUID runId, RunTrigger trigger, boolean dryRun, Instant startedAt) {
    RunContext context = new RunContext(runId, dryRun, peers);
    // THE ONLY THING A DEPENDENCY CHECK READS. A step is in this map when it FAILED (mapped to
    // itself) or when it was skipped BECAUSE something failed (mapped to that same origin, carried
    // forward so the third step in a chain still names the one that broke). A step that succeeded,
    // and a step the process chose not to make, are simply absent — which is what makes a POLICY
    // skip stop being contagious.
    Map<String, String> failureOrigin = new LinkedHashMap<>();
    List<String> lines = new ArrayList<>();
    List<String> failures = new ArrayList<>();
    boolean anyFailed = false;

    try {
      for (StepDefinition step : process.steps()) {
        String blocker = blocker(step, failureOrigin);
        StepResult result;
        if (blocker != null) {
          result = StepResult.skippedByFailure(blocker);
        } else {
          Instant stepStartedAt = Instant.now();
          runs.stepRunning(runId, step.id(), stepStartedAt);
          announceStep(process.kind(), runId, step.id(), RunStatus.RUNNING, stepStartedAt);
          result = run(step, context);
        }
        if (result.status() == RunStatus.FAILED) {
          anyFailed = true;
          failureOrigin.put(step.id(), step.id());
          failures.add(failure(step, result));
          context.recordFailure(step.id(), result.error());
        } else if (result.skip() == SkipKind.FAILURE) {
          failureOrigin.put(step.id(), blocker == null ? step.id() : blocker);
        }
        if (result.answer() != null) {
          context.record(step.id(), result.answer().json(), result.answer().body());
        }
        Instant stepFinishedAt = Instant.now();
        runs.stepFinished(runId, step.id(), result, stepFinishedAt);
        announceStep(process.kind(), runId, step.id(), result.status(), stepFinishedAt);
        lines.add(line(step, result));
      }
    } catch (RuntimeException e) {
      // A store that will not answer, or a bug. Either way the run has to end.
      LOG.errorf(e, "The %s run %s could not be completed", process.kind(), runId);
      anyFailed = true;
      lines.add("the run could not be completed: " + e);
    }

    RunStatus status = anyFailed ? RunStatus.FAILED : RunStatus.SUCCEEDED;
    String summary = String.join("; ", lines);
    Instant finishedAt = Instant.now();
    boolean closed = false;
    try {
      runs.close(runId, status, summary, finishedAt);
      closed = true;
    } catch (RuntimeException e) {
      // Nothing else can be done here: the account of the run is what could not be written.
      LOG.errorf(e, "The %s run %s finished %s but could not be closed", process.kind(), runId, status);
    }
    if (closed) {
      // Only a close that RETURNED is announced. The row of one that threw still says RUNNING, and
      // an event saying otherwise would be a claim the store does not back.
      RunAnnouncer.RunChange ended =
          new RunAnnouncer.RunChange(
              process.kind(), runId, status, trigger, dryRun, startedAt, finishedAt, finishedAt);
      announce(
          "the " + status + " of " + process.kind() + " run " + runId,
          to -> to.runChanged(ended));
    }
    if (status == RunStatus.FAILED) {
      // The steps that failed, not the whole summary: twenty clauses of which nineteen say "ok" or
      // "skipped" is the line nobody reads. A run that threw before any step failed has no such
      // step, and its summary — which ends in the exception — is the account instead.
      LOG.warn(
          process.kind()
              + " run "
              + runId
              + " FAILED: "
              + (failures.isEmpty() ? summary : String.join("; ", failures)));
    } else {
      LOG.infof("%s run %s %s: %s", process.kind(), runId, status, summary);
    }
  }

  /** One step's transition, told after the write that made it returned. Never throws. */
  private void announceStep(
      String kind, UUID runId, String stepId, RunStatus status, Instant changedAt) {
    RunAnnouncer.StepChange change =
        new RunAnnouncer.StepChange(kind, runId, stepId, status, changedAt);
    announce(
        "step " + stepId + " " + status + " of " + kind + " run " + runId,
        to -> to.stepChanged(change));
  }

  /**
   * Hands one change to the announcer, if there is one. <b>Never throws</b>: it runs on the worker
   * between store writes, and a throw there would skip the writes after it — a RUNNING row nobody
   * closes, and no second run of that kind ever again. An unresolvable announcer is the module on
   * its own, and announces nothing; an ambiguous one is a deployment mistake, and is a WARN per
   * change rather than a dead run.
   */
  private void announce(String what, Consumer<RunAnnouncer> call) {
    try {
      if (announcer.isUnsatisfied()) {
        return;
      }
      call.accept(announcer.get());
    } catch (RuntimeException e) {
      LOG.warnf(e, "Could not announce %s", what);
    }
  }

  /** How much of a failed step's response body the WARN line carries. */
  static final int FAILURE_BODY_CHARS = 300;

  /**
   * One FAILED step, as the WARN line names it: {@code <id> (<peer>): <error>[: <body start>]}.
   *
   * <p>The body because the error is this service's sentence and the body is the peer's: a 503 from
   * qits-maintenance says in its own words whether it is healing or broken, and that is what decides
   * whether a person needs to act.
   */
  static String failure(StepDefinition step, StepResult result) {
    StringBuilder line =
        new StringBuilder(step.id())
            .append(" (")
            .append(step.target())
            .append("): ")
            .append(result.error() == null ? result.status().name() : result.error());
    String body = result.answer() == null ? null : result.answer().body();
    if (body != null && !body.isBlank()) {
      String flat = body.strip().replaceAll("\\s+", " ");
      line.append(": ")
          .append(
              flat.length() <= FAILURE_BODY_CHARS
                  ? flat
                  : flat.substring(0, FAILURE_BODY_CHARS) + "…");
    }
    return line.toString();
  }

  /**
   * Which FAILED step blocks this one, or null.
   *
   * <p><b>Only a failure blocks.</b> A dependency the process chose not to make — {@code
   * artifacts.sweep} on a dry run — is a satisfied dependency: nothing went wrong, so nothing after
   * it has a reason not to run. Reading every non-SUCCEEDED dependency as broken is what made the
   * platform's first real dry run report {@code usage.after} as {@code skipped: artifacts.sweep
   * failed} while all nine calls had answered 200.
   *
   * <p>Dependencies are read in declaration order and the first blocker wins, so a step with two
   * broken predecessors names the earlier one — the same one a person reading the list top to
   * bottom would name.
   */
  private static String blocker(StepDefinition step, Map<String, String> failureOrigin) {
    for (String dependency : step.dependsOn()) {
      String origin = failureOrigin.get(dependency);
      if (origin != null) {
        return origin;
      }
    }
    return null;
  }

  /** One step's body, with a thrown exception turned into the FAILED row it should have been. */
  private static StepResult run(StepDefinition step, RunContext context) {
    try {
      return step.body().run(context);
    } catch (RuntimeException e) {
      return new StepResult(RunStatus.FAILED, null, null, null, step.id() + " threw: " + e, null);
    }
  }

  /** One step's contribution to the run's summary line. */
  private static String line(StepDefinition step, StepResult result) {
    String detail =
        switch (result.status()) {
          case SUCCEEDED -> result.summary() == null ? "ok" : result.summary();
          case SKIPPED, FAILED -> result.error() == null ? result.status().name() : result.error();
          default -> result.status().name();
        };
    return step.id() + " " + result.status() + " (" + detail + ")";
  }
}
