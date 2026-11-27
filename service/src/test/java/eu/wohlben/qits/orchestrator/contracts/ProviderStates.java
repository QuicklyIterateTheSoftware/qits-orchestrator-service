package eu.wohlben.qits.orchestrator.contracts;

import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerCall;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.ProcessRegistry;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * <b>qits-orchestrator's provider states</b> (ticket qits-1933): each stages what one consumer
 * situation needs and hands back its parameters.
 *
 * <p>Two callers. {@link GoldenMasterRecordingTest} runs a state before it records each operation
 * that names it, and {@link ConsumerPactVerificationTest}'s {@code @State} methods delegate here.
 *
 * <p><b>The service's state is its run log</b> — {@code op_run} and {@code op_step} — plus the
 * peers a started run calls, which the suite fakes at {@link FakePeers}. A state that needs a run
 * writes one through {@link RunStore}, the log's only writer, rather than running the executor: an
 * executed row carries whatever the fakes and the clock produced, a written one carries exactly
 * what is spelled below.
 *
 * <p><b>Every state drains first.</b> One run of a kind at a time is the service's rule (a second
 * start is a 409, {@code RunStore.open} refuses a second open), and one worker runs every kind, so
 * a run left RUNNING by the previous interaction would make the next state's own open or start fail
 * — or wait — for a reason that is not its own. {@link #A_RUNNING_GC_RUN} is the one state that
 * leaves a run RUNNING on purpose, and nothing on the executor will ever close it, so the drain
 * closes it ({@link #awaitNoActiveRun}).
 *
 * <p><b>The scan and SBOM check processes</b> (ticket qits-2490) have states of their own, each
 * naming its {@code kind}: the process ({@link #THE_SCAN_PROCESS}, {@link
 * #THE_SBOM_CHECK_PROCESS} — the listing, and the 400 a dry run of it is refused with), one
 * finished run of it ({@link #A_FINISHED_SCAN_RUN}, {@link #A_FINISHED_SBOM_CHECK_RUN}) and a
 * start that is accepted ({@link #NO_ACTIVE_SCAN_RUN}, {@link #NO_ACTIVE_SBOM_CHECK_RUN}).
 */
@ApplicationScoped
public class ProviderStates {

  public static final String THE_GC_PROCESS = "the gc process";
  public static final String THE_GC_PROCESS_BEFORE_ITS_FIRST_RUN =
      "the gc process before its first run";
  public static final String A_FINISHED_GC_DRY_RUN = "a finished gc dry run";
  public static final String A_RUNNING_GC_RUN = "a running gc run";
  public static final String NO_ACTIVE_GC_RUN = "no active gc run";
  public static final String A_REPOSITORY_THE_CLEANUP_CAN_PLAN = "a repository the cleanup can plan";
  public static final String THE_SCAN_PROCESS = "the scan process";
  public static final String A_FINISHED_SCAN_RUN = "a finished scan run";
  public static final String NO_ACTIVE_SCAN_RUN = "no active scan run";
  public static final String THE_SBOM_CHECK_PROCESS = "the sbom-check process";
  public static final String A_FINISHED_SBOM_CHECK_RUN = "a finished sbom-check run";
  public static final String NO_ACTIVE_SBOM_CHECK_RUN = "no active sbom-check run";

  /** The scan ids the seeded scan run's two requests were answered with. */
  static final String SEEDED_INTERNAL_SCAN = "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b";

  static final String SEEDED_EXTERNAL_SCAN = "7a2b3c4d-5e6f-4a70-8b8c-9d0e1f2a3b4c";

  /** The step {@link #A_FINISHED_SCAN_RUN} names as its {@code stepId}: a follow, with its read. */
  static final String SEEDED_SCAN_STEP_ID = ScanProcess.INTERNAL_FOLLOW;

  /** When the seeded scan run opened — the scan schedule's 00:30. */
  static final Instant SEEDED_SCAN_START = Instant.parse("2026-10-01T00:30:00Z");

  /** When the seeded SBOM check run opened — its schedule's 02:15. */
  static final Instant SEEDED_SBOM_CHECK_START = Instant.parse("2026-10-01T02:15:00Z");

  /** What the seeded SBOM check's one call was answered: one version without an SBOM, ticketed. */
  static final String SEEDED_SBOM_CHECK_REPORT =
      "{\"ranAt\":\"2026-10-01T02:15:00Z\",\"filed\":true,\"entries\":[{\"project\":\"qits\","
          + "\"repository\":\"qits-ci-service\",\"ecosystem\":\"maven\","
          + "\"name\":\"eu.wohlben.qits:qits-ci\",\"version\":\"2026.930.1\","
          + "\"reason\":\"MISSING\"}],\"warnings\":[],\"tickets\":[{\"project\":\"qits\","
          + "\"ecosystem\":\"maven\",\"name\":\"eu.wohlben.qits:qits-ci\","
          + "\"ticketSlug\":\"qits-703\",\"versions\":[\"2026.930.1\"]}]}";

  /** The repository {@link #A_REPOSITORY_THE_CLEANUP_CAN_PLAN} names (its param). */
  static final String CLEANUP_REPOSITORY = "daemons";

  /** The step {@link #A_FINISHED_GC_DRY_RUN} names as its {@code stepId}: a POST with a body. */
  static final String SEEDED_STEP_ID = "artifacts.plan";

  /**
   * How many of {@link #SEEDED_STEPS} {@link #A_RUNNING_GC_RUN} has finished: the next one is
   * RUNNING and the rest PENDING. The first three are the usage reads and the deployment pins.
   */
  static final int RUNNING_RUN_FINISHED_STEPS = 3;

  /** When the seeded run opened; every step and the close are offsets from it. */
  static final Instant SEEDED_START = Instant.parse("2026-10-01T03:00:00Z");

  /**
   * One step of the seeded run, as it ended: its call ({@code method} and {@code path}, the url
   * resolved on the step's target as the executor resolves it), the peer's short answer and the
   * step's human line. A null {@code method} is the step a dry run does not make.
   */
  record SeededStep(String method, String path, String response, String summary) {}

  /**
   * The seeded dry run, step by step, keyed by the step ids {@link GcProcess} declares and in its
   * order. A definition that gains, loses or reorders a step fails the state, rather than seeding a
   * run the process could not have produced. A POST sends {@code {"dryRun":true}}, which is all a
   * consumer reading the run is shown of the bodies the real steps build.
   */
  static final Map<String, SeededStep> SEEDED_STEPS = seededSteps();

  /** What a state hands back: its parameters, keys sorted. */
  public record Setup(Map<String, String> params) {}

  private final Map<String, Supplier<Setup>> states = new LinkedHashMap<>();

  @Inject ProcessRegistry registry;

  @Inject GcProcess gc;

  @Inject ScanProcess scan;

  @Inject SbomCheckProcess sbomCheck;

  @Inject RunStore runs;

  @Inject FakePeers peers;

  /**
   * The runs {@link #A_RUNNING_GC_RUN} left RUNNING. No executor is driving them, so {@link
   * #awaitNoActiveRun} closes them rather than waiting for an ending that never comes.
   */
  private final Set<UUID> seededRunning = Collections.synchronizedSet(new LinkedHashSet<>());

  public ProviderStates() {
    states.put(THE_GC_PROCESS, this::theGcProcess);
    states.put(THE_GC_PROCESS_BEFORE_ITS_FIRST_RUN, this::theGcProcessBeforeItsFirstRun);
    states.put(A_FINISHED_GC_DRY_RUN, this::aFinishedGcDryRun);
    states.put(A_RUNNING_GC_RUN, this::aRunningGcRun);
    states.put(NO_ACTIVE_GC_RUN, this::noActiveGcRun);
    states.put(A_REPOSITORY_THE_CLEANUP_CAN_PLAN, this::aRepositoryTheCleanupCanPlan);
    states.put(THE_SCAN_PROCESS, () -> theProcess(ScanProcess.KIND));
    states.put(A_FINISHED_SCAN_RUN, this::aFinishedScanRun);
    states.put(NO_ACTIVE_SCAN_RUN, this::noActiveScanRun);
    states.put(THE_SBOM_CHECK_PROCESS, () -> theProcess(SbomCheckProcess.KIND));
    states.put(A_FINISHED_SBOM_CHECK_RUN, this::aFinishedSbomCheckRun);
    states.put(NO_ACTIVE_SBOM_CHECK_RUN, this::noActiveSbomCheckRun);
  }

  /** Every state name this provider answers for. */
  public Set<String> names() {
    return Collections.unmodifiableSet(states.keySet());
  }

  /** Runs the named state; an unknown name is a programming error, not an empty state. */
  public Setup setUp(String state) {
    Supplier<Setup> setup = states.get(state);
    if (setup == null) {
      throw new IllegalArgumentException(
          "No provider state '" + state + "' — this provider answers for " + states.keySet());
    }
    return setup.get();
  }

  /** {@link #setUp} for a pact {@code @State} method, which returns only the params. */
  public Map<String, String> params(String state) {
    return setUp(state).params();
  }

  /** The state's slug: lower-cased, every run of non-alphanumerics replaced by {@code -}. */
  public static String slug(String state) {
    return state.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");
  }

  /**
   * Waits until no run of any kind is RUNNING. A state calls it on the way in; a caller that started
   * a run calls it on the way out, so the next class against this application does not get a 409
   * that is not its own. The same wait as {@code ProcessApiTest.awaitNoActiveRun}, and the same
   * clear: the reader's session would otherwise keep answering the row it first read. A run {@link
   * #A_RUNNING_GC_RUN} seeded is closed FAILED first: it is on no executor.
   */
  public void awaitNoActiveRun() {
    synchronized (seededRunning) {
      for (UUID seeded : seededRunning) {
        runs.close(
            seeded, RunStatus.FAILED, "closed by the provider state that seeded it", Instant.now());
      }
      seededRunning.clear();
    }
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      runs.getEntityManager().clear();
      if (registry.all().stream().allMatch(process -> runs.active(process.kind()).isEmpty())) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("a run never finished");
  }

  // --- the states ------------------------------------------------------------------------------

  /**
   * The service's real registry, nothing seeded: {@code GET /processes} answers every process with
   * its steps and edges — gc, and since ticket qits-2490 the scan and SBOM check processes beside
   * it. It checks that the registry holds exactly those three — {@code ProbeProcess} lives in the
   * {@code orchestrator} module's test sources, off this classpath, and a recording that listed a
   * test-only process would publish it as the contract. Its param is the gc process's {@code kind}.
   */
  private Setup theGcProcess() {
    return theProcess(GcProcess.KIND);
  }

  /** The registry as {@link #theGcProcess} checks it, with {@code kind} as the param. */
  private Setup theProcess(String kind) {
    List<String> kinds = registry.all().stream().map(TechnicalProcess::kind).toList();
    List<String> expected = List.of(GcProcess.KIND, SbomCheckProcess.KIND, ScanProcess.KIND);
    if (!kinds.equals(expected)) {
      throw new IllegalStateException(
          "The process states expect the processes " + expected + ", the registry holds " + kinds);
    }
    return params(Map.of("kind", kind));
  }

  /**
   * One gc run, started by a person ({@code manual}), dry, SUCCEEDED, with every step's row as a
   * dry run leaves it: each step that called its peer SUCCEEDED with its request, the peer's 200
   * and a summary, and {@code artifacts.sweep} SKIPPED for {@code dry run}. Written through {@link
   * RunStore} — {@code open}, {@code stepRunning}, {@code stepFinished}, {@code close} — at fixed
   * instants from {@link #SEEDED_START}. Its params are the {@code kind}, the {@code runId} {@code
   * GET /runs/{id}} takes and the {@code stepId} {@code GET /runs/{id}/steps/{stepId}} takes
   * ({@link #SEEDED_STEP_ID}, ticket qits-2359). Applying it twice writes a second run; each
   * answers for itself.
   *
   * <p><b>It forgets every other gc run first</b> ({@link #forgetRuns}, ticket qits-2733): {@code
   * listProcessRuns} answers only the newest {@code DEFAULT_LIMIT} runs, and this seeded run's
   * instant is fixed in the past, so without that it falls off the page once twenty newer gc runs
   * exist — as the suite's own earlier interactions and a bumped {@code QITS_DEPLOYED_PACTS_DIR}
   * both produce.
   */
  private Setup aFinishedGcDryRun() {
    awaitNoActiveRun();
    forgetRuns(GcProcess.KIND);
    List<StepDefinition> steps = definedSteps(A_FINISHED_GC_DRY_RUN);
    UUID runId = runs.open(GcProcess.KIND, RunTrigger.MANUAL, true, steps, SEEDED_START);
    List<String> lines = new ArrayList<>();
    Instant at = SEEDED_START;
    for (StepDefinition step : steps) {
      SeededStep seeded = SEEDED_STEPS.get(step.id());
      // As the executor writes it: RUNNING first, the ending a second later — a policy skip too.
      runs.stepRunning(runId, step.id(), at);
      at = at.plusSeconds(1);
      StepResult result;
      if (seeded.method() == null) {
        result = StepResult.skipped("dry run");
        lines.add(step.id() + " " + result.status() + " (" + result.error() + ")");
      } else {
        PeerCall call =
            new PeerCall(
                seeded.method(),
                peers.url(step.target(), seeded.path()),
                // qits-toggles' door takes its flag in the query and no body (qits-2703).
                "POST".equals(seeded.method()) && !step.id().equals("toggles.retirement")
                    ? "{\"dryRun\":true}"
                    : null);
        result =
            new StepResult(
                RunStatus.SUCCEEDED,
                call,
                new PeerAnswer(200, seeded.response(), null, null),
                seeded.summary(),
                null,
                null);
        lines.add(step.id() + " " + result.status() + " (" + result.summary() + ")");
      }
      runs.stepFinished(runId, step.id(), result, at);
    }
    runs.close(runId, RunStatus.SUCCEEDED, String.join("; ", lines), at.plusSeconds(1));
    Map<String, String> params = new TreeMap<>();
    params.put("kind", GcProcess.KIND);
    params.put("runId", runId.toString());
    params.put("stepId", SEEDED_STEP_ID);
    return params(params);
  }

  /**
   * The gc process as it stands on a fresh install (ticket qits-2359): registered, and not one run
   * of it recorded, so {@code GET /processes/gc/runs} answers {@code []}. It deletes every gc run
   * row the run log holds — the earlier states' and interactions' — because a consumer pacting
   * against "no runs yet" is shown an empty list and nothing else. Its param is the {@code kind}.
   */
  private Setup theGcProcessBeforeItsFirstRun() {
    awaitNoActiveRun();
    forgetRuns(GcProcess.KIND);
    return params(Map.of("kind", GcProcess.KIND));
  }

  /**
   * Deletes every run of {@code kind} the run log holds, steps and all — so a state that seeds one
   * fixed run afterwards is not sharing a {@code listProcessRuns} page with whatever earlier states
   * and interactions left behind (ticket qits-2733: the listing answers only the newest {@code
   * DEFAULT_LIMIT}, and a fixed, past instant otherwise sorts behind them).
   */
  private void forgetRuns(String kind) {
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              OpStep.delete("runId in (select r.id from OpRun r where r.kind = ?1)", kind);
              OpRun.delete("kind", kind);
            });
    runs.getEntityManager().clear();
  }

  /**
   * One gc run in flight (ticket qits-2359): started by a person ({@code manual}), real rather than
   * dry, RUNNING with no {@code finishedAt} and no {@code summary}. Its first {@value
   * #RUNNING_RUN_FINISHED_STEPS} steps SUCCEEDED with {@link #SEEDED_STEPS}' calls and answers, the
   * next one is RUNNING (started, nothing sent back yet) and the rest are PENDING — the page a
   * client polls every two seconds. A {@code POST /processes/gc/runs} is refused with a 409 naming
   * this run. Its params are the {@code kind} and the {@code runId}.
   *
   * <p><b>Its instants are the clock's, not {@link #SEEDED_START}'s</b>: {@code RunStore.open}
   * treats a RUNNING row older than {@code abandon-after} as abandoned and opens over it, which
   * would turn the 409 into a 202. The recording freezes them all the same. It leaves the run
   * RUNNING; {@link #awaitNoActiveRun} closes it.
   */
  private Setup aRunningGcRun() {
    awaitNoActiveRun();
    List<StepDefinition> steps = definedSteps(A_RUNNING_GC_RUN);
    Instant at = Instant.now().minusSeconds(60);
    UUID runId = runs.open(GcProcess.KIND, RunTrigger.MANUAL, false, steps, at);
    seededRunning.add(runId);
    for (int i = 0; i <= RUNNING_RUN_FINISHED_STEPS; i++) {
      StepDefinition step = steps.get(i);
      runs.stepRunning(runId, step.id(), at);
      at = at.plusSeconds(1);
      if (i == RUNNING_RUN_FINISHED_STEPS) {
        break; // the step in flight: started, not ended
      }
      SeededStep seeded = SEEDED_STEPS.get(step.id());
      PeerCall call =
          new PeerCall(seeded.method(), peers.url(step.target(), seeded.path()), null);
      runs.stepFinished(
          runId,
          step.id(),
          new StepResult(
              RunStatus.SUCCEEDED,
              call,
              new PeerAnswer(200, seeded.response(), null, null),
              seeded.summary(),
              null,
              null),
          at);
    }
    Map<String, String> params = new TreeMap<>();
    params.put("kind", GcProcess.KIND);
    params.put("runId", runId.toString());
    return params(params);
  }

  /** The gc process's steps, refused unless they are exactly {@link #SEEDED_STEPS}' ids in order. */
  private List<StepDefinition> definedSteps(String state) {
    List<StepDefinition> steps = gc.steps();
    List<String> ids = steps.stream().map(StepDefinition::id).toList();
    if (!ids.equals(List.copyOf(SEEDED_STEPS.keySet()))) {
      throw new IllegalStateException(
          "State '"
              + state
              + "' seeds the steps "
              + SEEDED_STEPS.keySet()
              + ", the gc process declares "
              + ids);
    }
    return steps;
  }

  /**
   * No gc run RUNNING, and every peer scripted with the happy path — the answers {@code
   * ProcessApiTest.scriptTheHappyPath} gives — so a {@code POST /processes/gc/runs} is a 202 and
   * the run it starts SUCCEEDS without reaching a network. Its param is the {@code kind}. The run
   * it starts is the caller's to wait out ({@link #awaitNoActiveRun}).
   */
  private Setup noActiveGcRun() {
    awaitNoActiveRun();
    peers.reset();
    peers.answer(
        "/containers/api/gc/usage",
        FakePeers.Scripted.ok(
            "{\"images\":{\"sizeBytes\":10,\"reclaimableBytes\":2},\"buildCache\":{\"sizeBytes\":4}}"));
    peers.answer(
        "/artifacts/api/store/summary",
        FakePeers.Scripted.ok(
            "{\"diskTotalBytes\":10,\"ociUnionBytes\":8,\"docsBytes\":1,\"sbomBytes\":1}"));
    peers.answer(
        "/deployments/api/pins",
        FakePeers.Scripted.ok("{\"pins\":[{\"applicationName\":\"qits-ci\",\"shas\":[\"abc\"]}]}"));
    peers.answer(
        "/maintenance/api/pins",
        FakePeers.Scripted.ok(
            "{\"repositories\":[{\"name\":\"qits-ci-service\"}],"
                + "\"pins\":[{\"ecosystem\":\"maven\",\"name\":\"eu.wohlben.qits:qits-blobstore\","
                + "\"version\":\"1\",\"repository\":\"qits-ci-service\"}]}"));
    peers.answer(
        "/workspaces/api/pins",
        FakePeers.Scripted.ok(
            "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\"0\","
                + "\"launches\":\"workspace\"}]}"));
    peers.answer(
        "/projects/api/pins",
        FakePeers.Scripted.ok(
            "{\"pins\":[{\"image\":\"qits/project-agent\",\"version\":\"0\","
                + "\"launches\":\"agent\"}]}"));
    peers.answer(
        "/ci/api/daemon",
        FakePeers.Scripted.ok("{\"daemonName\":\"qits-ci-daemon\",\"daemonVersion\":\"1\"}"));
    peers.answer(
        "/artifacts/api/gc/plan", FakePeers.Scripted.ok("{\"summary\":{\"executable\":true}}"));
    peers.answer(
        "/artifacts/api/gc/sweep", FakePeers.Scripted.ok("{\"sweep\":{\"blobsUnlinked\":0}}"));
    peers.answer(
        "/containers/api/gc/images", FakePeers.Scripted.ok("{\"removed\":[],\"kept\":[]}"));
    peers.answer(
        "/containers/api/gc/volumes", FakePeers.Scripted.ok("{\"removed\":[],\"kept\":[]}"));
    peers.answer(
        "/containers/api/gc/build-cache",
        FakePeers.Scripted.ok("{\"host\":{\"reclaimedBytes\":0},\"builders\":[]}"));
    peers.answer(
        "/projects/api/repositories",
        FakePeers.Scripted.ok(
            "{\"repositories\":[{\"id\":\"r-1\",\"projectId\":\"p-1\",\"name\":\"qits-ci\","
                + "\"mainBranch\":\"main\"}]}"));
    peers.answer(
        "/workspaces/api/gc/branches",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"repositoriesExamined\":1,\"branchesExamined\":4,"
                + "\"removed\":[{\"repositoryId\":\"r-1\",\"repositoryName\":\"qits-ci\","
                + "\"branch\":\"old-work\"}],\"errors\":[]}"));
    peers.answer(
        "/projects/api/gc/tags",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"repositories\":1,\"examined\":1,"
                + "\"deleted\":[{\"repository\":\"qits-ci\",\"tag\":\"old\",\"host\":true,"
                + "\"twin\":true}],"
                + "\"kept\":{\"newest\":0,\"pinnedVersion\":1,\"gitlink\":0,\"inFlight\":0,"
                + "\"young\":0},\"errors\":[]}"));
    peers.answer(
        "/deployments/api/claims/idp-clients",
        FakePeers.Scripted.ok(
            "{\"claims\":[{\"clientId\":\"dev-qits-ci\",\"applicationName\":\"qits-ci\","
                + "\"environmentName\":\"dev\",\"createdAt\":\"2026-09-01T10:00:00Z\"}]}"));
    peers.answer(
        "/idp/api/gc/service-clients",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"removed\":[],"
                + "\"kept\":[{\"clientId\":\"dev-qits-ci\",\"reason\":\"claimed\"}],"
                + "\"keptCounts\":{\"claimed\":1,\"grace\":0,\"caller\":0}}"));
    peers.answer(
        "/maintenance/api/pins/images",
        FakePeers.Scripted.ok("[{\"name\":\"node\",\"version\":\"24-alpine\"}]"));
    peers.answer(
        "/mirror/api/gc/sweep",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"pins\":{\"supplied\":true,\"images\":1},\"types\":[],"
                + "\"blobs\":{\"unlinked\":0,\"reclaimedBytes\":0}}"));
    peers.answer(
        "/toggles/api/retirement/runs?dryRun=true",
        FakePeers.Scripted.ok(
            "{\"dryRun\":true,\"used\":[],\"filed\":[],\"kept\":[],\"dropped\":[],\"failed\":[]}"));
    peers.answer(
        "/toggles/api/retirement/runs",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"used\":[],\"filed\":[],\"kept\":[],\"dropped\":[],\"failed\":[]}"));
    return params(Map.of("kind", GcProcess.KIND));
  }

  /**
   * The artifacts cleanup listing's state (ticket qits-2138): no gc run RUNNING, the five pin sources
   * answering, and qits-artifacts' supplied-pins listing answering — so {@code GET
   * /artifacts/repositories} answers 200 with qits-artifacts' text. Its param is the {@code
   * repository} the listing names (the plan and the sweep are gone, qits-2477).
   *
   * <p><b>qits-artifacts' answers are its own golden masters</b>, copied verbatim into {@code
   * src/test/resources/contracts/artifacts-cleanup/} from its state {@code a repository with
   * unpinned identities} (qits-artifacts-service 858f107, re-copied for qits-2329 once the plan
   * recorded its dead list, blob figures and configuration): the doors hand qits-artifacts' text on
   * untouched, so what a consumer of these doors pacts against is exactly what qits-artifacts
   * recorded. A copy rather than the published jar, because the service module does not carry
   * qits-artifacts' golden masters and the pinned version predates the three operations. When
   * qits-artifacts re-records them, copy them again.
   */
  private Setup aRepositoryTheCleanupCanPlan() {
    answerCleanupPins();
    peers.answer(
        "/artifacts/api/gc/repositories",
        FakePeers.Scripted.ok(artifactsAnswer("planGcRepositoriesWithSuppliedPins")));
    return params(Map.of("repository", CLEANUP_REPOSITORY));
  }

  /** No gc run active, and the five pin sources answering as the cleanup reads them. */
  private void answerCleanupPins() {
    awaitNoActiveRun();
    peers.reset();
    peers.answer(
        "/deployments/api/pins",
        FakePeers.Scripted.ok("{\"pins\":[{\"applicationName\":\"qits-ci\",\"shas\":[\"abc\"]}]}"));
    peers.answer(
        "/ci/api/daemon",
        FakePeers.Scripted.ok("{\"daemonName\":\"qits-ci-daemon\",\"daemonVersion\":\"1\"}"));
    peers.answer(
        "/maintenance/api/pins",
        FakePeers.Scripted.ok("{\"repositories\":[{\"name\":\"qits-ci-service\"}],\"pins\":[]}"));
    peers.answer("/workspaces/api/pins", FakePeers.Scripted.ok("{\"pins\":[]}"));
    peers.answer("/projects/api/pins", FakePeers.Scripted.ok("{\"pins\":[]}"));
  }

  /** One of qits-artifacts' recorded answers, as the copy under {@code contracts/} holds it. */
  private static String artifactsAnswer(String operationId) {
    String resource = "/contracts/artifacts-cleanup/" + operationId + ".json";
    try (java.io.InputStream in = ProviderStates.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("no " + resource + " on the test classpath");
      }
      return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  // --- the scan and SBOM check processes (qits-2490) -------------------------------------------

  /**
   * One scan run, started by the clock ({@code scheduled}), SUCCEEDED: each scan requested (202,
   * its id) and followed to SUCCEEDED, written through {@link RunStore} at fixed instants from
   * {@link #SEEDED_SCAN_START}. Its params are the {@code kind}, the {@code runId} and the {@code
   * stepId} {@code GET /runs/{id}/steps/{stepId}} takes ({@link #SEEDED_SCAN_STEP_ID}).
   *
   * <p>It forgets every other scan run first ({@link #forgetRuns}, ticket qits-2733) for the same
   * reason {@link #aFinishedGcDryRun} does: the listing's page is twenty, and this run's instant is
   * fixed in the past.
   */
  private Setup aFinishedScanRun() {
    awaitNoActiveRun();
    forgetRuns(ScanProcess.KIND);
    Map<String, SeededStep> seeded = new LinkedHashMap<>();
    seeded.put(
        ScanProcess.INTERNAL_REQUEST,
        new SeededStep(
            "POST",
            ScanProcess.SCANS_PATH,
            "{\"id\":\"" + SEEDED_INTERNAL_SCAN + "\"}",
            "queued scan " + SEEDED_INTERNAL_SCAN));
    seeded.put(
        ScanProcess.INTERNAL_FOLLOW,
        new SeededStep(
            "GET",
            ScanProcess.SCANS_PATH + "/" + SEEDED_INTERNAL_SCAN,
            scanAnswer(SEEDED_INTERNAL_SCAN, "INTERNAL"),
            "INTERNAL scan SUCCEEDED"));
    seeded.put(
        ScanProcess.EXTERNAL_REQUEST,
        new SeededStep(
            "POST",
            ScanProcess.SCANS_PATH,
            "{\"id\":\"" + SEEDED_EXTERNAL_SCAN + "\"}",
            "queued scan " + SEEDED_EXTERNAL_SCAN));
    seeded.put(
        ScanProcess.EXTERNAL_FOLLOW,
        new SeededStep(
            "GET",
            ScanProcess.SCANS_PATH + "/" + SEEDED_EXTERNAL_SCAN,
            scanAnswer(SEEDED_EXTERNAL_SCAN, "EXTERNAL"),
            "EXTERNAL scan SUCCEEDED"));
    Map<String, String> bodies =
        Map.of(
            ScanProcess.INTERNAL_REQUEST, "{\"scope\":\"INTERNAL\"}",
            ScanProcess.EXTERNAL_REQUEST, "{\"scope\":\"EXTERNAL\"}");
    UUID runId = seedFinishedRun(scan, seeded, bodies, SEEDED_SCAN_START);
    Map<String, String> params = new TreeMap<>();
    params.put("kind", ScanProcess.KIND);
    params.put("runId", runId.toString());
    params.put("stepId", SEEDED_SCAN_STEP_ID);
    return params(params);
  }

  /**
   * One SBOM check run, started by the clock ({@code scheduled}), SUCCEEDED: its one call answered
   * 202 with {@link #SEEDED_SBOM_CHECK_REPORT}. Its params are the {@code kind}, the {@code runId}
   * and the {@code stepId} ({@code check}).
   *
   * <p>It forgets every other sbom-check run first ({@link #forgetRuns}, ticket qits-2733) for the
   * same reason {@link #aFinishedGcDryRun} does: the listing's page is twenty, and this run's
   * instant is fixed in the past.
   */
  private Setup aFinishedSbomCheckRun() {
    awaitNoActiveRun();
    forgetRuns(SbomCheckProcess.KIND);
    Map<String, SeededStep> seeded = new LinkedHashMap<>();
    seeded.put(
        SbomCheckProcess.CHECK,
        new SeededStep(
            "POST",
            SbomCheckProcess.RUNS_PATH,
            SEEDED_SBOM_CHECK_REPORT,
            "1 version without a usable SBOM, 1 ticket open, 0 warnings"));
    UUID runId = seedFinishedRun(sbomCheck, seeded, Map.of(), SEEDED_SBOM_CHECK_START);
    Map<String, String> params = new TreeMap<>();
    params.put("kind", SbomCheckProcess.KIND);
    params.put("runId", runId.toString());
    params.put("stepId", SbomCheckProcess.CHECK);
    return params(params);
  }

  /**
   * No run RUNNING, and qits-maintenance's scan door scripted to queue a scan and answer it
   * SUCCEEDED at the first read — so a {@code POST /processes/scan/runs} is a 202 and the run it
   * starts SUCCEEDS at once, without a network or a wait. Its param is the {@code kind}.
   */
  private Setup noActiveScanRun() {
    awaitNoActiveRun();
    peers.reset();
    peers.answer(
        ScanProcess.SCANS_PATH,
        FakePeers.Scripted.status(202, "{\"id\":\"" + SEEDED_INTERNAL_SCAN + "\"}"));
    peers.answer(
        ScanProcess.SCANS_PATH + "/" + SEEDED_INTERNAL_SCAN,
        FakePeers.Scripted.ok(scanAnswer(SEEDED_INTERNAL_SCAN, "INTERNAL")));
    return params(Map.of("kind", ScanProcess.KIND));
  }

  /**
   * No run RUNNING, and qits-maintenance's check door scripted with {@link
   * #SEEDED_SBOM_CHECK_REPORT} — so a {@code POST /processes/sbom-check/runs} is a 202 and the run it
   * starts SUCCEEDS. Its param is the {@code kind}.
   */
  private Setup noActiveSbomCheckRun() {
    awaitNoActiveRun();
    peers.reset();
    peers.answer(
        SbomCheckProcess.RUNS_PATH, FakePeers.Scripted.status(202, SEEDED_SBOM_CHECK_REPORT));
    return params(Map.of("kind", SbomCheckProcess.KIND));
  }

  /** A finished scan as qits-maintenance's {@code getScan} answers one. */
  private static String scanAnswer(String id, String scope) {
    return "{\"id\":\""
        + id
        + "\",\"scope\":\""
        + scope
        + "\",\"repository\":null,\"trigger\":\"MANUAL\",\"status\":\"SUCCEEDED\","
        + "\"startedAt\":\"2026-10-01T00:30:01Z\",\"finishedAt\":\"2026-10-01T00:31:00Z\","
        + "\"message\":null}";
  }

  /**
   * One SUCCEEDED, scheduled run of {@code process}, every step as {@code seeded} spells it and in
   * the process's own order — a definition that gains, loses or reorders a step fails the state.
   * Each step RUNNING first and ended a second later, as the executor writes it; a POST carries its
   * {@code bodies} entry. The run's id.
   */
  private UUID seedFinishedRun(
      TechnicalProcess process,
      Map<String, SeededStep> seeded,
      Map<String, String> bodies,
      Instant start) {
    List<StepDefinition> steps = process.steps();
    List<String> ids = steps.stream().map(StepDefinition::id).toList();
    if (!ids.equals(List.copyOf(seeded.keySet()))) {
      throw new IllegalStateException(
          "The seeded "
              + process.kind()
              + " run has the steps "
              + seeded.keySet()
              + ", the process declares "
              + ids);
    }
    UUID runId = runs.open(process.kind(), RunTrigger.SCHEDULED, false, steps, start);
    List<String> lines = new ArrayList<>();
    Instant at = start;
    for (StepDefinition step : steps) {
      SeededStep one = seeded.get(step.id());
      runs.stepRunning(runId, step.id(), at);
      at = at.plusSeconds(1);
      PeerCall call =
          new PeerCall(one.method(), peers.url(step.target(), one.path()), bodies.get(step.id()));
      int status = "POST".equals(one.method()) ? 202 : 200;
      StepResult result =
          new StepResult(
              RunStatus.SUCCEEDED,
              call,
              new PeerAnswer(status, one.response(), null, null),
              one.summary(),
              null,
              null);
      lines.add(step.id() + " " + result.status() + " (" + result.summary() + ")");
      runs.stepFinished(runId, step.id(), result, at);
    }
    runs.close(runId, RunStatus.SUCCEEDED, String.join("; ", lines), at.plusSeconds(1));
    return runId;
  }

  private static Setup params(Map<String, String> params) {
    return new Setup(Collections.unmodifiableMap(new TreeMap<>(params)));
  }

  private static Map<String, SeededStep> seededSteps() {
    Map<String, SeededStep> steps = new LinkedHashMap<>();
    steps.put(
        "usage.before",
        new SeededStep(
            "GET",
            "/containers/api/gc/usage",
            "{\"images\":{\"sizeBytes\":10,\"reclaimableBytes\":2},\"buildCache\":{\"sizeBytes\":4}}",
            "images 10 B (2 B reclaimable), build cache 4 B"));
    steps.put(
        "artifacts.usage.before",
        new SeededStep(
            "GET",
            "/artifacts/api/store/summary",
            "{\"diskTotalBytes\":10,\"ociUnionBytes\":8,\"docsBytes\":1,\"sbomBytes\":1}",
            "store 10 B: oci 8 B, docs 1 B, sbom 1 B"));
    steps.put(
        "pins.deployments",
        new SeededStep(
            "GET",
            "/deployments/api/pins",
            "{\"pins\":[{\"applicationName\":\"qits-ci\",\"shas\":[\"abc\"]}]}",
            "1 deployment pin across 1 application"));
    steps.put(
        "pins.ci",
        new SeededStep(
            "GET",
            "/ci/api/daemon",
            "{\"daemonName\":\"qits-ci-daemon\",\"daemonVersion\":\"1\"}",
            "qits-ci-daemon 1"));
    steps.put(
        "pins.dependencies",
        new SeededStep(
            "GET",
            "/maintenance/api/pins",
            "{\"repositories\":[{\"name\":\"qits-ci-service\"}],\"pins\":[]}",
            "0 manifest pins across 1 repository"));
    steps.put(
        "pins.workspaces",
        new SeededStep("GET", "/workspaces/api/pins", "{\"pins\":[]}", "0 workspace launch pins"));
    steps.put(
        "pins.projects",
        new SeededStep("GET", "/projects/api/pins", "{\"pins\":[]}", "0 project launch pins"));
    steps.put(
        "pins.mirror-images",
        new SeededStep(
            "GET",
            "/maintenance/api/pins/images",
            "[{\"name\":\"node\",\"version\":\"24-alpine\"}]",
            "1 image pin — what qits-mirror keeps its mirrored tags by"));
    steps.put(
        "artifacts.plan",
        new SeededStep(
            "POST",
            "/artifacts/api/gc/plan",
            "{\"summary\":{\"executable\":true}}",
            "plan executable"));
    steps.put("artifacts.sweep", new SeededStep(null, null, null, null));
    steps.put(
        "containers.images",
        new SeededStep(
            "POST",
            "/containers/api/gc/images",
            "{\"removed\":[],\"kept\":[]}",
            "removed 0 images, kept 0"));
    steps.put(
        "containers.volumes",
        new SeededStep(
            "POST",
            "/containers/api/gc/volumes",
            "{\"removed\":[],\"kept\":[]}",
            "removed 0 volumes, kept 0"));
    steps.put(
        "containers.build-cache",
        new SeededStep(
            "POST",
            "/containers/api/gc/build-cache",
            "{\"host\":{\"reclaimedBytes\":0},\"builders\":[]}",
            "reclaimed 0 B"));
    steps.put(
        "repos.catalogue",
        new SeededStep(
            "GET",
            "/projects/api/repositories",
            "{\"repositories\":[]}",
            "0 repositories"));
    steps.put(
        "branches.sweep",
        new SeededStep(
            "POST",
            "/workspaces/api/gc/branches",
            "{\"dryRun\":true,\"removed\":[],\"errors\":[]}",
            "removed 0 of 0 branches"));
    steps.put(
        "tags.sweep",
        new SeededStep(
            "POST",
            "/projects/api/gc/tags",
            "{\"dryRun\":true,\"deleted\":[],\"errors\":[]}",
            "0 tags decommissioned"));
    steps.put(
        "claims.idp-clients",
        new SeededStep(
            "GET",
            "/deployments/api/claims/idp-clients",
            "{\"claims\":[]}",
            "0 service-client claims"));
    steps.put(
        "idp.service-clients",
        new SeededStep(
            "POST",
            "/idp/api/gc/service-clients",
            "{\"dryRun\":true,\"removed\":[],\"kept\":[]}",
            "removed 0 service clients"));
    steps.put(
        "mirror.sweep",
        new SeededStep(
            "POST",
            "/mirror/api/gc/sweep",
            "{\"dryRun\":true,\"pins\":{\"supplied\":true,\"images\":1},\"types\":[],"
                + "\"blobs\":{\"reclaimable\":0,\"reclaimableBytes\":0}}",
            "would delete 0 cached entries"));
    steps.put(
        "artifacts.usage.after",
        new SeededStep(
            "GET",
            "/artifacts/api/store/summary",
            "{\"diskTotalBytes\":10,\"ociUnionBytes\":8,\"docsBytes\":1,\"sbomBytes\":1}",
            "store 10 B: oci 8 B, docs 1 B, sbom 1 B"));
    steps.put(
        "usage.after",
        new SeededStep(
            "GET",
            "/containers/api/gc/usage",
            "{\"images\":{\"sizeBytes\":10,\"reclaimableBytes\":2},\"buildCache\":{\"sizeBytes\":4}}",
            "images 10 B (2 B reclaimable), build cache 4 B"));
    steps.put(
        "toggles.retirement",
        new SeededStep(
            "POST",
            "/toggles/api/retirement/runs?dryRun=true",
            "{\"dryRun\":true,\"at\":\"2026-01-01T03:00:00Z\",\"used\":[],\"filed\":[{\"slug\":"
                + "\"toggles-smoke\",\"ticket\":null,\"unusedSince\":\"2025-12-29T03:00:00Z\"}],"
                + "\"kept\":[],\"dropped\":[],\"failed\":[]}",
            "would file 1 removal ticket (toggles-smoke); kept 0; would drop 0; 0 toggles used by"
                + " a deployed pact; 0 failed"));
    return Collections.unmodifiableMap(steps);
  }
}
