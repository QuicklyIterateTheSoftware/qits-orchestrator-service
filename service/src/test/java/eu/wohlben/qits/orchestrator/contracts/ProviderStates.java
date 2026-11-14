package eu.wohlben.qits.orchestrator.contracts;

import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerCall;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.ProcessRegistry;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
 * <p><b>Every state drains first.</b> One gc run at a time is the service's rule (a second start is
 * a 409, {@code RunStore.open} refuses a second open), so a run left RUNNING by the previous
 * interaction would make the next state's own open or start fail for a reason that is not its own.
 */
@ApplicationScoped
public class ProviderStates {

  public static final String THE_GC_PROCESS = "the gc process";
  public static final String A_FINISHED_GC_DRY_RUN = "a finished gc dry run";
  public static final String NO_ACTIVE_GC_RUN = "no active gc run";
  public static final String A_REPOSITORY_THE_CLEANUP_CAN_PLAN = "a repository the cleanup can plan";

  /** The repository {@link #A_REPOSITORY_THE_CLEANUP_CAN_PLAN} plans and sweeps. */
  static final String CLEANUP_REPOSITORY = "daemons";

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

  @Inject RunStore runs;

  @Inject FakePeers peers;

  public ProviderStates() {
    states.put(THE_GC_PROCESS, this::theGcProcess);
    states.put(A_FINISHED_GC_DRY_RUN, this::aFinishedGcDryRun);
    states.put(NO_ACTIVE_GC_RUN, this::noActiveGcRun);
    states.put(A_REPOSITORY_THE_CLEANUP_CAN_PLAN, this::aRepositoryTheCleanupCanPlan);
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
   * Waits until no gc run is RUNNING. A state calls it on the way in; a caller that started a run
   * calls it on the way out, so the next class against this application does not get a 409 that is
   * not its own. The same wait as {@code ProcessApiTest.awaitNoActiveGcRun}, and the same clear: the
   * reader's session would otherwise keep answering the row it first read.
   */
  public void awaitNoActiveGcRun() {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      runs.getEntityManager().clear();
      if (runs.active(GcProcess.KIND).isEmpty()) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("a gc run never finished");
  }

  // --- the states ------------------------------------------------------------------------------

  /**
   * The service's real registry, nothing seeded: {@code GET /processes} answers the gc process
   * with its steps and edges. It checks that the registry holds the gc process and nothing else —
   * {@code ProbeProcess} lives in the {@code orchestrator} module's test sources, off this
   * classpath, and a recording that listed a test-only process would publish it as the contract.
   * Its param is the process's {@code kind}.
   */
  private Setup theGcProcess() {
    List<String> kinds = registry.all().stream().map(TechnicalProcess::kind).toList();
    if (!kinds.equals(List.of(GcProcess.KIND))) {
      throw new IllegalStateException(
          "State '" + THE_GC_PROCESS + "' expects the gc process alone, the registry holds " + kinds);
    }
    return params(Map.of("kind", GcProcess.KIND));
  }

  /**
   * One gc run, started by a person ({@code manual}), dry, SUCCEEDED, with every step's row as a
   * dry run leaves it: each step that called its peer SUCCEEDED with its request, the peer's 200
   * and a summary, and {@code artifacts.sweep} SKIPPED for {@code dry run}. Written through {@link
   * RunStore} — {@code open}, {@code stepRunning}, {@code stepFinished}, {@code close} — at fixed
   * instants from {@link #SEEDED_START}. Its params are the {@code kind} and the {@code runId}
   * {@code GET /runs/{id}} takes. Applying it twice writes a second run; each answers for itself.
   */
  private Setup aFinishedGcDryRun() {
    awaitNoActiveGcRun();
    List<StepDefinition> steps = gc.steps();
    List<String> ids = steps.stream().map(StepDefinition::id).toList();
    if (!ids.equals(List.copyOf(SEEDED_STEPS.keySet()))) {
      throw new IllegalStateException(
          "State '"
              + A_FINISHED_GC_DRY_RUN
              + "' seeds the steps "
              + SEEDED_STEPS.keySet()
              + ", the gc process declares "
              + ids);
    }
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
                "POST".equals(seeded.method()) ? "{\"dryRun\":true}" : null);
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
    return params(params);
  }

  /**
   * No gc run RUNNING, and every peer scripted with the happy path — the answers {@code
   * ProcessApiTest.scriptTheHappyPath} gives — so a {@code POST /processes/gc/runs} is a 202 and
   * the run it starts SUCCEEDS without reaching a network. Its param is the {@code kind}. The run
   * it starts is the caller's to wait out ({@link #awaitNoActiveGcRun}).
   */
  private Setup noActiveGcRun() {
    awaitNoActiveGcRun();
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
    return params(Map.of("kind", GcProcess.KIND));
  }

  /**
   * The artifacts cleanup doors' state (ticket qits-2138): no gc run RUNNING, the five pin sources
   * answering, and qits-artifacts' three supplied-pins doors answering for the repository {@code
   * daemons} — so {@code GET /artifacts/repositories}, {@code GET
   * /artifacts/repositories/daemons/plan} and {@code POST /artifacts/repositories/daemons/sweep}
   * answer 200 with qits-artifacts' text. Its param is the {@code repository}.
   *
   * <p><b>qits-artifacts' answers are its own golden masters</b>, copied verbatim into {@code
   * src/test/resources/contracts/artifacts-cleanup/} from its state {@code a repository with
   * unpinned identities} (qits-artifacts-service 858f107): the doors hand qits-artifacts' text on
   * untouched, so what a consumer of these doors pacts against is exactly what qits-artifacts
   * recorded. A copy rather than the published jar, because the service module does not carry
   * qits-artifacts' golden masters and the pinned version predates the three operations. When
   * qits-artifacts re-records them, copy them again.
   */
  private Setup aRepositoryTheCleanupCanPlan() {
    awaitNoActiveGcRun();
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
    peers.answer(
        "/artifacts/api/gc/repositories",
        FakePeers.Scripted.ok(artifactsAnswer("planGcRepositoriesWithSuppliedPins")));
    peers.answer(
        "/artifacts/api/gc/repositories/" + CLEANUP_REPOSITORY + "/plan",
        FakePeers.Scripted.ok(artifactsAnswer("planGcRepositoryWithSuppliedPins")));
    peers.answer(
        "/artifacts/api/gc/repositories/" + CLEANUP_REPOSITORY + "/sweep",
        FakePeers.Scripted.ok(artifactsAnswer("sweepGcRepository")));
    return params(Map.of("repository", CLEANUP_REPOSITORY));
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
    return Collections.unmodifiableMap(steps);
  }
}
