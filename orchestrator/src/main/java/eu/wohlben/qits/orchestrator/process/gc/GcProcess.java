package eu.wohlben.qits.orchestrator.process.gc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * The gc process: the platform's one deletion run, across every store that grows.
 *
 * <h2>Who deletes what</h2>
 *
 * Nothing here. Registry blobs, tags and manifests are qits-artifacts' own GC engine; host images,
 * orphan volumes and buildkit cache are qits-containers', which is the component that holds the
 * platform's docker socket. This process reads the pin set, hands it to the deleters and records
 * what each one said.
 *
 * <h2>The pins, and why they are read here</h2>
 *
 * <p>A pin is something that must survive a collection: an image sha a restart or a rollback would
 * pull ({@code pins.deployments}), the ci daemon binary a run would launch ({@code pins.ci}), a
 * version a repository's main still references in a manifest ({@code pins.dependencies}) and the
 * image a launch would pull TODAY ({@code pins.workspaces}, {@code pins.projects}). They are read ONCE per run and
 * handed to every deleter, so every deleter judges against one answer taken at one moment.
 *
 * <p><b>The last three are CONSUMPTION rather than deployment</b>, which is why neither of the first
 * two could stand in for them: nothing deploys a library a pom pins, and nothing deploys the
 * workspace image a person's next click pulls. A release-age rule cannot see either, so before
 * these steps existed the only thing keeping them alive was how recently somebody happened to ask
 * for them.
 *
 * <p><b>Five sources, and no two of them are the same claim.</b> {@code pins.deployments} is what
 * serves right now; {@code pins.ci} is what a run launches; {@code pins.dependencies} is what source
 * still references; {@code pins.workspaces} and {@code pins.projects} are what a launch pulls TODAY,
 * read by the service that would do the pulling, out of the config it is actually running with.
 * Image versions come from each service's own config, built from its pom, so each launching service
 * answers for itself. (A sixth source, {@code pins.images} from qits-configuration, was removed in
 * qits-1176: that service no longer holds image names.)
 *
 * <p><b>They are read here because this is the one component that holds an idp client for every
 * peer.</b> qits-artifacts' own HTTP pin readers stay as its no-body fallback, and they are exactly
 * what was 401-ing: a credential-less reader of a guarded peer. A supplied pin set moves that read
 * to the caller that can authenticate.
 *
 * <p><b>Fail-closed, and the edges are what makes it so.</b> Every step that deletes depends on all
 * FIVE pin reads ({@link #PIN_SOURCES}), so one failed pin read skips every deleter in every store —
 * the registry, host images, volumes, build cache, branches, configuration entries, tags and
 * service clients alike. Only the reads and the opening measurements still run. A pin read FAILS
 * not only when its peer cannot be reached or refuses, but also when its answer cannot be read
 * whole — cut off, or not JSON ({@link StepResult#ofKeepSet}). Before ticket qits-1175 a cut-off
 * answer SUCCEEDED as an empty pin set, and only qits-artifacts' own empty-source guard stopped a
 * plan that condemned everything maintenance pins.
 *
 * <p><b>A 503 is asked again before it counts</b> (ticket qits-1733, {@link #keepSet}). Each pin
 * read is one GET, and a peer that answers 503 is asked again, up to {@code
 * qits.orchestrator.gc.pins.retry.attempts} reads in all, {@code ...pins.retry.delay} apart.
 * qits-maintenance answers 503 for the seconds its pin set is incomplete and heals it in the
 * background; read once, that window was a FAILED run that deleted nothing, twelve times in one
 * week. Only the 503 is re-asked: another status, an unreachable peer and an unreadable 2xx fail at
 * once, and the last 503 fails the step — so fail-closed is exactly what it was, only later.
 *
 * <h2>The steps</h2>
 *
 * <pre>
 * usage.before           containers     GET  /containers/api/gc/usage
 * artifacts.usage.before artifacts      GET  /artifacts/api/store/summary
 * pins.deployments       deployments    GET  /deployments/api/pins
 * pins.ci                ci             GET  /ci/api/daemon
 * pins.dependencies      maintenance    GET  /maintenance/api/pins
 * pins.workspaces        workspaces     GET  /workspaces/api/pins
 * pins.projects          projects       GET  /projects/api/pins
 * artifacts.plan         artifacts      POST /artifacts/api/gc/plan {pins}   ← pins.*
 * artifacts.sweep        artifacts      POST /artifacts/api/gc/sweep {pins}  ← artifacts.plan, pins.*
 * containers.images      containers     POST /containers/api/gc/images       ← pins.*
 * containers.volumes     containers     POST /containers/api/gc/volumes      ← usage.before, pins.*
 * containers.build-cache containers     POST /containers/api/gc/build-cache  ← usage.before, pins.*
 * repos.catalogue        projects       GET  /projects/api/repositories
 * branches.sweep         workspaces     POST /workspaces/api/gc/branches     ← repos.catalogue, pins.*
 * configuration.entries  configuration  POST /configuration/api/gc/entries   ← pins.*
 * tags.sweep             projects       POST /projects/api/gc/tags {pins}    ← pins.*, repos.catalogue
 * claims.idp-clients     deployments    GET  /deployments/api/claims/idp-clients
 * idp.service-clients    idp            POST /idp/api/gc/service-clients     ← claims.idp-clients, pins.*
 * artifacts.usage.after  artifacts      GET  /artifacts/api/store/summary    ← artifacts.sweep
 * usage.after            containers     GET  /containers/api/gc/usage        ← everything that frees disk
 * </pre>

 * <p><b>{@code pins.workspaces} and {@code pins.projects} ride peers this process already drives.</b>
 * qits-workspaces is the branch sweep's peer and qits-projects the catalogue's, so the two effective
 * pin reads cost no target url, no oidc client and no new configuration — only a second path on a
 * socket that was already open. That is the whole reason the pattern scales: the service that would
 * do the pulling is, on this platform, usually a service the orchestrator can already authenticate
 * to.
 *
 * <p><b>The branch sweep is the same pin pattern, one store further out.</b> The repository
 * catalogue is its keep-set's complement — the iteration set — read here for the same reason the
 * pins are, and handed to qits-workspaces, which owns branch semantics: what a branch's parent is,
 * whether a workspace stands on it, and the single cleanup criterion every UI click already goes
 * through. A failed catalogue read skips the sweep (nothing deletes over a list it could not
 * read); {@code usage.after} does not wait for it, because deleted refs free no docker disk.
 *
 * <p>{@code usage.before} and {@code usage.after} are the same call twice, and the pair is the
 * point: the run's own measurement of what it achieved, taken by the component that owns the store
 * rather than added up from what each step claimed.
 *
 * <p><b>{@code artifacts.usage.*} is the second plane of the same measurement, and it exists
 * because the first one cannot see it.</b> The registry's bytes are rows in qits-artifacts' postgres
 * and files behind them — invisible to the docker read, which knows only the host's images,
 * containers, volumes and cache. A run whose whole receipt was {@code docker system df} therefore
 * reported a platform that was not growing while the registry did: the 2026-09-04 storage incident
 * was 50 GB nobody's receipt showed. Its after-step hangs off the registry sweep alone rather than
 * off everything that frees disk, so a broken container prune still leaves the registry's own
 * before-and-after in the run.
 *
 * <p><b>{@code configuration.entries} deletes rows, not bytes, which is why it hangs off neither
 * {@code usage.after} plane.</b> qits-configuration deletes entries of RETIRED keys — a key some
 * declaration of an application used to state and no surviving declaration states any longer. The
 * rule for what counts as retired lives with qits-configuration, the owner of the store; this
 * process hands it the one pin that answers the question its rule needs — {@code pins.deployments},
 * the serving and rollback shas — verbatim, the same discipline as {@link #pinsBody}. The other four
 * pin sources answer a different tense (what a launch would pull, what a manifest still references)
 * that has no bearing on whether a configuration key is still declared — but it still depends on
 * all five, because a run with any pin source unread deletes nothing (qits-1175). It runs on a dry run too — qits-configuration judges identically and deletes nothing, so the
 * nightly dry figures are real figures, the same reasoning as {@code branches.sweep}.
 *
 * <p><b>{@code tags.sweep} decommissions git tags, on the platform's git host and on its backup
 * twin alike.</b> qits-projects owns the repository catalogue and is the one peer that can reach
 * both addresses, so it is the one asked, the same reasoning that put {@code repos.catalogue} and
 * {@code branches.sweep} on their own owners. It carries every pin this run read — all FIVE, the
 * same {@code pins} object {@link #pinsBody} sends — because a tag can be the thing any one of them
 * protects: a sha a deployment still serves, a version a dependency manifest references, or an
 * image a launch would pull. {@code repos.catalogue} is the iteration set, the same role it plays
 * for the branch sweep. Fail-closed applies to all six edges alike: an unread pin, or an unread
 * catalogue, skips the sweep before its body runs. It is not a {@code usage.after} dependency either
 * — a decommissioned tag frees no docker disk — and it runs on a dry run too, the same reasoning as
 * {@code branches.sweep} and {@code configuration.entries}.
 *
 * <p><b>{@code idp.service-clients} deletes service clients, and its keep-set is a CLAIM rather
 * than a pin</b> (ticket qits-878). Every application that declares an {@code idp:client} resource
 * has qits-deployments provision a client for it; when the application goes away, the client stays
 * in qits-idp with nothing to say it is dead. qits-deployments is the one service that knows which
 * client ids are still claimed, so {@code claims.idp-clients} reads them and the sweep hands its
 * {@code claims} array to qits-idp VERBATIM — the owner of the store judges what is unclaimed,
 * what is still in its grace window and which client is the caller itself. Fail-closed is the same
 * edge as everywhere else: a failed claims read, or any failed pin read, skips the sweep before its
 * body runs. And it is
 * stricter than an edge on one point: a read that SUCCEEDED with zero claims is refused in the body
 * rather than sent, because an empty claim set would condemn every service client on the platform —
 * this orchestrator's own included — and a live platform always has at least that one claim. It
 * runs on a dry run too; qits-idp honours the flag itself. It frees no disk, so it hangs off
 * neither {@code usage.after} plane.
 */
@ApplicationScoped
public class GcProcess implements TechnicalProcess {

  /** The kind, in every url and in every stored row. */
  public static final String KIND = "gc";

  static final String USAGE_BEFORE = "usage.before";
  static final String ARTIFACTS_USAGE_BEFORE = "artifacts.usage.before";
  static final String PINS_DEPLOYMENTS = "pins.deployments";
  static final String PINS_CI = "pins.ci";
  static final String PINS_DEPENDENCIES = "pins.dependencies";
  static final String PINS_WORKSPACES = "pins.workspaces";
  static final String PINS_PROJECTS = "pins.projects";
  static final String ARTIFACTS_PLAN = "artifacts.plan";
  static final String ARTIFACTS_SWEEP = "artifacts.sweep";
  static final String CONTAINERS_IMAGES = "containers.images";
  static final String CONTAINERS_VOLUMES = "containers.volumes";
  static final String CONTAINERS_BUILD_CACHE = "containers.build-cache";
  static final String REPOS_CATALOGUE = "repos.catalogue";
  static final String BRANCHES_SWEEP = "branches.sweep";
  static final String CONFIGURATION_ENTRIES = "configuration.entries";
  static final String TAGS_SWEEP = "tags.sweep";
  static final String CLAIMS_IDP_CLIENTS = "claims.idp-clients";
  static final String IDP_SERVICE_CLIENTS = "idp.service-clients";
  static final String ARTIFACTS_USAGE_AFTER = "artifacts.usage.after";
  static final String USAGE_AFTER = "usage.after";

  /**
   * The five pin reads. Every step that deletes depends on ALL of them (ticket qits-1175): a run
   * with one pin source unread deletes nothing in any store, not only in the stores that source
   * protects. A pin source added here is an edge added to every deleter at once.
   */
  static final List<String> PIN_SOURCES =
      List.of(
          PINS_DEPLOYMENTS, PINS_CI, PINS_DEPENDENCIES, PINS_WORKSPACES, PINS_PROJECTS);

  /** A deleter's own edge first, then every pin source. */
  private static List<String> withPins(String own) {
    List<String> edges = new java.util.ArrayList<>();
    edges.add(own);
    edges.addAll(PIN_SOURCES);
    return edges;
  }

  /** Every pin source, then the deleter's own edge — the order tags.sweep has always listed. */
  private static List<String> pinsThen(String own) {
    List<String> edges = new java.util.ArrayList<>(PIN_SOURCES);
    edges.add(own);
    return edges;
  }

  private static final String USAGE_PATH = "/containers/api/gc/usage";

  private static final String STORE_PATH = "/artifacts/api/store/summary";

  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject GcConfig config;

  /**
   * How a pin read waits between two asks. A seam rather than a {@code Thread.sleep} inline, so a
   * suite that drives the shipped thirty-second delay never actually waits it out.
   */
  @FunctionalInterface
  interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    Sleeper THREAD = duration -> Thread.sleep(duration.toMillis());
  }

  /** The real wait in production; a test outside CDI swaps in its own. */
  Sleeper sleeper = Sleeper.THREAD;

  @Override
  public String kind() {
    return KIND;
  }

  @Override
  public String name() {
    return "Garbage collection";
  }

  @Override
  public String description() {
    return "Reads the platform's pin set once, then asks every store's own owner to delete what"
        + " nothing pins: registry identities and blobs (qits-artifacts), host images, orphan"
        + " volumes and buildkit cache (qits-containers), and service clients nothing claims"
        + " (qits-idp). Measures host disk and the registry"
        + " store before and after.";
  }

  @Override
  public List<StepDefinition> steps() {
    return List.of(
        new StepDefinition(
            USAGE_BEFORE,
            "Disk usage before",
            PeerTarget.CONTAINERS,
            List.of(),
            context ->
                StepResult.of(
                    context.peers().get(PeerTarget.CONTAINERS, USAGE_PATH),
                    answer -> GcSummaries.usage(answer.json()))),
        new StepDefinition(
            ARTIFACTS_USAGE_BEFORE,
            "Registry store before",
            PeerTarget.ARTIFACTS,
            List.of(),
            context ->
                StepResult.of(
                    context.peers().get(PeerTarget.ARTIFACTS, STORE_PATH),
                    answer -> GcSummaries.storeUsage(answer.json()))),
        new StepDefinition(
            PINS_DEPLOYMENTS,
            "Deployment pins",
            PeerTarget.DEPLOYMENTS,
            List.of(),
            context ->
                keepSet(
                    context,
                    PeerTarget.DEPLOYMENTS,
                    "/deployments/api/pins",
                    answer -> GcSummaries.deploymentPins(answer.json()))),
        new StepDefinition(
            PINS_CI,
            "CI daemon pin",
            PeerTarget.CI,
            List.of(),
            context ->
                keepSet(
                    context,
                    PeerTarget.CI,
                    "/ci/api/daemon",
                    answer -> GcSummaries.ciPin(answer.json()))),
        new StepDefinition(
            PINS_DEPENDENCIES,
            "Dependency pins",
            PeerTarget.MAINTENANCE,
            List.of(),
            context ->
                keepSet(
                    context,
                    PeerTarget.MAINTENANCE,
                    "/maintenance/api/pins",
                    answer -> GcSummaries.dependencyPins(answer.json()))),
        new StepDefinition(
            PINS_WORKSPACES,
            "Workspace launch pins",
            PeerTarget.WORKSPACES,
            List.of(),
            context ->
                keepSet(
                    context,
                    PeerTarget.WORKSPACES,
                    "/workspaces/api/pins",
                    answer -> GcSummaries.workspaceLaunchPins(answer.json()))),
        new StepDefinition(
            PINS_PROJECTS,
            "Project launch pins",
            PeerTarget.PROJECTS,
            List.of(),
            context ->
                keepSet(
                    context,
                    PeerTarget.PROJECTS,
                    "/projects/api/pins",
                    answer -> GcSummaries.projectLaunchPins(answer.json()))),
        new StepDefinition(
            ARTIFACTS_PLAN,
            "Plan the registry collection",
            PeerTarget.ARTIFACTS,
            PIN_SOURCES,
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(PeerTarget.ARTIFACTS, "/artifacts/api/gc/plan", pinsBody(context)),
                    answer -> GcSummaries.artifactsPlan(answer.json()))),
        new StepDefinition(
            ARTIFACTS_SWEEP,
            "Sweep the registry",
            PeerTarget.ARTIFACTS,
            // Every pin read as well as the plan, because this is the step that DELETES and its body
            // is its own pinsBody rather than the plan's. The plan edge already carries all five
            // transitively; naming them here is the doctrine rather than the mechanism — a step that
            // deletes on the strength of a pin carries that pin's edge, so a source added to the
            // body is a source that cannot be added without this list noticing.
            withPins(ARTIFACTS_PLAN),
            context ->
                context.dryRun()
                    ? StepResult.skipped("dry run")
                    : StepResult.of(
                        context
                            .peers()
                            .post(
                                PeerTarget.ARTIFACTS, "/artifacts/api/gc/sweep", pinsBody(context)),
                        answer -> GcSummaries.artifactsSweep(answer.json()))),
        new StepDefinition(
            CONTAINERS_IMAGES,
            "Collect host images",
            PeerTarget.CONTAINERS,
            PIN_SOURCES,
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(PeerTarget.CONTAINERS, "/containers/api/gc/images", imagesBody(context)),
                    answer -> GcSummaries.images(answer.json()))),
        new StepDefinition(
            CONTAINERS_VOLUMES,
            "Collect orphan volumes",
            PeerTarget.CONTAINERS,
            withPins(USAGE_BEFORE),
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(
                            PeerTarget.CONTAINERS,
                            "/containers/api/gc/volumes",
                            volumesBody(context)),
                    answer -> GcSummaries.volumes(answer.json()))),
        new StepDefinition(
            CONTAINERS_BUILD_CACHE,
            "Prune the build cache",
            PeerTarget.CONTAINERS,
            // usage.before and the pins, NOT containers.images. A prune needs no pin set of its own,
            // but a run with an unread pin source deletes nothing anywhere (qits-1175). Declaration
            // order still puts it after the image sweep, which is all the ordering it ever wanted.
            withPins(USAGE_BEFORE),
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(
                            PeerTarget.CONTAINERS,
                            "/containers/api/gc/build-cache",
                            buildCacheBody(context)),
                    answer -> GcSummaries.buildCache(answer.json()))),
        new StepDefinition(
            REPOS_CATALOGUE,
            "Repository catalogue",
            PeerTarget.PROJECTS,
            List.of(),
            context ->
                StepResult.of(
                    context.peers().get(PeerTarget.PROJECTS, "/projects/api/repositories"),
                    answer -> GcSummaries.repositoryCatalogue(answer.json()))),
        new StepDefinition(
            BRANCHES_SWEEP,
            "Sweep merged branches",
            PeerTarget.WORKSPACES,
            withPins(REPOS_CATALOGUE),
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(
                            PeerTarget.WORKSPACES,
                            "/workspaces/api/gc/branches",
                            branchesBody(context)),
                    answer -> GcSummaries.branchesSweep(answer.json()))),
        new StepDefinition(
            CONFIGURATION_ENTRIES,
            "Retired configuration entries",
            PeerTarget.CONFIGURATION,
            PIN_SOURCES,
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(
                            PeerTarget.CONFIGURATION,
                            "/configuration/api/gc/entries",
                            entriesBody(context)),
                    answer -> GcSummaries.configurationEntries(answer.json()))),
        new StepDefinition(
            TAGS_SWEEP,
            "Decommissioned git tags",
            PeerTarget.PROJECTS,
            pinsThen(REPOS_CATALOGUE),
            context ->
                StepResult.of(
                    context
                        .peers()
                        .post(
                            PeerTarget.PROJECTS,
                            "/projects/api/gc/tags",
                            tagsSweepBody(context),
                            config.tagsSweepCallTimeout()),
                    answer -> GcSummaries.tagsSweep(answer.json()))),
        new StepDefinition(
            CLAIMS_IDP_CLIENTS,
            "Service-client claims",
            PeerTarget.DEPLOYMENTS,
            List.of(),
            context ->
                StepResult.of(
                    context
                        .peers()
                        .get(PeerTarget.DEPLOYMENTS, "/deployments/api/claims/idp-clients"),
                    answer -> GcSummaries.idpClientClaims(answer.json()))),
        new StepDefinition(
            IDP_SERVICE_CLIENTS,
            "Unclaimed service clients",
            PeerTarget.IDP,
            withPins(CLAIMS_IDP_CLIENTS),
            GcProcess::serviceClients),
        new StepDefinition(
            ARTIFACTS_USAGE_AFTER,
            "Registry store after",
            PeerTarget.ARTIFACTS,
            // The registry sweep alone. The docker measurement below waits on everything that frees
            // host disk; the registry's own bytes are freed by exactly one step, so a container
            // prune that failed must not cost the run the one figure that would have shown the
            // registry shrinking.
            List.of(ARTIFACTS_SWEEP),
            context ->
                StepResult.of(
                    context.peers().get(PeerTarget.ARTIFACTS, STORE_PATH),
                    answer -> GcSummaries.storeUsage(answer.json()))),
        new StepDefinition(
            USAGE_AFTER,
            "Disk usage after",
            PeerTarget.CONTAINERS,
            List.of(ARTIFACTS_SWEEP, CONTAINERS_IMAGES, CONTAINERS_VOLUMES, CONTAINERS_BUILD_CACHE),
            context ->
                StepResult.of(
                    context.peers().get(PeerTarget.CONTAINERS, USAGE_PATH),
                    answer -> GcSummaries.usage(answer.json()))));
  }

  /**
   * One {@code pins.*} read: a GET judged by {@link StepResult#ofKeepSet}, asked again while the
   * peer answers 503 (ticket qits-1733).
   *
   * <p><b>503 alone, because 503 alone says "ask again".</b> qits-maintenance answers it while its
   * pin set is not complete, and its body says the next read answers once it is; the heal lands in
   * seconds. Any other non-2xx, a peer that cannot be reached, and a 2xx that cannot be read whole
   * are answers about the peer rather than about a moment, so they fail on the first read as they
   * always have. Up to {@link GcConfig#pinsRetryAttempts()} reads in all, {@link
   * GcConfig#pinsRetryDelay()} apart; the last 503 FAILS the step, and the edges do the rest.
   *
   * <p><b>The row is the LAST read's</b> — its call, its answer, its verdict — with the count beside
   * it when there was more than one: {@code (after 2 attempts)} on the summary, {@code (6 attempts,
   * 30s apart)} after the error. The error keeps its own text as the prefix, so {@code answered 503}
   * still reads the way every reader of a failed pin row already matches it.
   *
   * <p>An interrupt stops the asking: the flag is restored and the last read is the verdict.
   */
  private StepResult keepSet(
      RunContext context, String target, String path, Function<PeerAnswer, String> summary) {
    int attempts = config.pinsRetryAttempts();
    Duration delay = config.pinsRetryDelay();
    StepResult result = StepResult.ofKeepSet(context.peers().get(target, path), summary);
    int made = 1;
    while (made < attempts && answered503(result)) {
      try {
        sleeper.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      result = StepResult.ofKeepSet(context.peers().get(target, path), summary);
      made++;
    }
    if (made == 1) {
      return result;
    }
    if (result.status() == RunStatus.SUCCEEDED) {
      return new StepResult(
          result.status(),
          result.call(),
          result.answer(),
          result.summary() + " (after " + made + " attempts)",
          result.error(),
          result.skip());
    }
    return new StepResult(
        result.status(),
        result.call(),
        result.answer(),
        result.summary(),
        result.error() + " (" + made + " attempts, " + spelled(delay) + " apart)",
        result.skip());
  }

  /** A FAILED read whose peer answered, and answered 503 — the one answer worth asking again. */
  private static boolean answered503(StepResult result) {
    PeerAnswer answer = result.answer();
    return result.status() == RunStatus.FAILED
        && answer != null
        && answer.error() == null
        && Integer.valueOf(503).equals(answer.httpStatus());
  }

  /** {@code 30s}, or {@code 250ms} for a delay that is not whole seconds. */
  static String spelled(Duration delay) {
    long millis = delay.toMillis();
    return millis % 1000 == 0 ? (millis / 1000) + "s" : millis + "ms";
  }

  /**
   * The pin set, as qits-artifacts takes it:
   *
   * <pre>
   * {"pins": {"deployments":      &lt;the deployments answer, verbatim&gt;,
   *           "ciDaemon":         &lt;the ci answer, verbatim&gt;,
   *           "dependencies":     &lt;the maintenance answer, verbatim&gt;,
   *           "workspaceLaunches":&lt;the workspaces answer, verbatim&gt;,
   *           "projectLaunches":  &lt;the projects answer, verbatim&gt;}}
   * </pre>
   *
   * <p><b>Verbatim means the bytes the peer sent.</b> Each member is the other service's whole
   * response document, re-embedded rather than re-shaped: the five contracts belong to
   * qits-platform-deployments, qits-ci, qits-platform-maintenance, qits-workspaces and qits-projects,
   * and a projection here would be a sixth copy of them to keep in step.
   *
   * <p><b>A member this run could not read is ABSENT, not null.</b> qits-artifacts reads a missing
   * member as that pin source being unanswered, which makes the plan not executable and aborts a
   * sweep — the fail-closed rule, unchanged. In practice the edges mean this step never runs
   * without all five, so the absent case is the belt.
   */
  private static String pinsBody(RunContext context) {
    ObjectNode body = JSON.createObjectNode();
    body.set("pins", pinsObject(context));
    return body.toString();
  }

  /**
   * The five pin answers, re-embedded verbatim under the names qits-artifacts (and now
   * qits-projects' tag sweep) take them by. Shared by every step that sends the whole pin set
   * rather than a single source of it, so the five lines live in exactly one place.
   */
  private static ObjectNode pinsObject(RunContext context) {
    ObjectNode pins = JSON.createObjectNode();
    context.answer(PINS_DEPLOYMENTS).ifPresent(node -> pins.set("deployments", node));
    context.answer(PINS_CI).ifPresent(node -> pins.set("ciDaemon", node));
    context.answer(PINS_DEPENDENCIES).ifPresent(node -> pins.set("dependencies", node));
    context.answer(PINS_WORKSPACES).ifPresent(node -> pins.set("workspaceLaunches", node));
    context.answer(PINS_PROJECTS).ifPresent(node -> pins.set("projectLaunches", node));
    return pins;
  }

  /**
   * The host image request:
   *
   * <pre>{@code {"dryRun":…, "minAge":"PT6H", "keep":[…], "keepPrefixes":[…]}}</pre>
   *
   * <p><b>The keep-set is the deployments pin answer, translated into local tags.</b> A deployment
   * pins {@code (applicationName, shas)}; the tag the host carries for one is
   * {@code qits/<applicationName>:<sha>} — the spelling {@code .config/qits/ci-post-receive.yml}
   * pushes and the deployer pulls. The prefixes beside it are configuration: build images and the
   * native toolchain image, which no deployment pins and an age rule condemns first.
   *
   * <p>A pin answer this run could not read leaves {@code keep} EMPTY — but this step never runs in
   * that case, because {@code pins.deployments} is its declared dependency and a failed dependency
   * skips it. The two together are why an empty keep-set can never reach qits-containers.
   */
  private String imagesBody(RunContext context) {
    ArrayNode keep = JSON.createArrayNode();
    context
        .answer(PINS_DEPLOYMENTS)
        .ifPresent(
            answer -> {
              for (JsonNode pin : answer.path("pins")) {
                String application = pin.path("applicationName").asText(null);
                if (application == null || application.isBlank()) {
                  continue;
                }
                for (JsonNode sha : pin.path("shas")) {
                  String value = sha.asText(null);
                  if (value != null && !value.isBlank()) {
                    keep.add("qits/" + application + ":" + value);
                  }
                }
              }
            });

    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.put("minAge", config.imageMinAge().toString());
    body.set("keep", keep);
    ArrayNode prefixes = JSON.createArrayNode();
    config.imageKeepPrefixes().forEach(prefixes::add);
    body.set("keepPrefixes", prefixes);
    return body.toString();
  }

  /**
   * The branch sweep request:
   *
   * <pre>{@code {"dryRun":…, "repositories":[{"id","name","mainBranch"}…], "keepPrefixes":[…]}}</pre>
   *
   * <p><b>A projection this time, not the verbatim document</b> — the request shape belongs to
   * qits-workspaces' own contract ({@code SweepRepository}), so the catalogue rows are respelled
   * into it rather than re-embedded; the three fields are exactly what the sweep judges by. Unlike
   * the artifacts sweep, this one runs on a dry run too: qits-workspaces judges identically and
   * deletes nothing, so the nightly dry figures are real figures.
   */
  private String branchesBody(RunContext context) {
    ArrayNode repositories = JSON.createArrayNode();
    context
        .answer(REPOS_CATALOGUE)
        .ifPresent(
            answer -> {
              for (JsonNode row : answer.path("repositories")) {
                String id = row.path("id").asText(null);
                if (id == null || id.isBlank()) {
                  continue;
                }
                ObjectNode repository = repositories.addObject();
                repository.put("id", id);
                repository.put("name", row.path("name").asText(""));
                repository.put("mainBranch", row.path("mainBranch").asText(""));
              }
            });
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.set("repositories", repositories);
    ArrayNode prefixes = JSON.createArrayNode();
    config.branchKeepPrefixes().forEach(prefixes::add);
    body.set("keepPrefixes", prefixes);
    return body.toString();
  }

  /**
   * The configuration entry-gc request:
   *
   * <pre>{@code {"dryRun":…, "deployments": <the deployments pin answer, verbatim>}}</pre>
   *
   * <p><b>Verbatim, the same discipline as {@link #pinsBody}.</b> qits-configuration owns the
   * retirement rule, so the deployments pin travels as the document qits-platform-deployments sent
   * rather than a keep-set re-shaped here. A pin answer this run could not read is simply absent —
   * but this step never runs in that case, because {@code pins.deployments} is its declared
   * dependency.
   */
  private String entriesBody(RunContext context) {
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    context.answer(PINS_DEPLOYMENTS).ifPresent(node -> body.set("deployments", node));
    return body.toString();
  }

  /**
   * The tag-decommission request:
   *
   * <pre>{@code {"dryRun":…, "pins": {"deployments":…, "ciDaemon":…, "dependencies":…,
   * "workspaceLaunches":…, "projectLaunches":…}}}</pre>
   *
   * <p><b>The same {@code pins} object {@link #pinsBody} sends, built by {@link #pinsObject} so the
   * five lines live once.</b> A git tag can be the thing any one of the five protects — a sha a
   * deployment still serves, a version a manifest references, an image a launch would pull — so
   * qits-projects is handed every source rather than one, unlike {@link #entriesBody}'s single pin.
   * A member this run could not read is simply absent, the same fail-closed discipline as the
   * artifacts plan — but this step never runs with one missing, because all five pin reads are its
   * declared dependencies.
   */
  private String tagsSweepBody(RunContext context) {
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.set("pins", pinsObject(context));
    return body.toString();
  }

  /**
   * The service-client sweep:
   *
   * <pre>{@code {"dryRun":…, "claims": <the claims read's "claims" array, verbatim>}}</pre>
   *
   * <p><b>Verbatim, the same discipline as {@link #entriesBody}</b>: the claim rows are
   * qits-deployments' contract and qits-idp reads them as such, so they are re-embedded rather than
   * re-shaped into a list of ids here.
   *
   * <p><b>Never an empty list.</b> The edge already skips this step when the read FAILED; this is
   * the other half — a read that answered with no claims (or none this run could parse) is refused
   * before a request exists, because to qits-idp an empty claim set reads as "nothing is claimed".
   * qits-idp answers 400 to one as well, and a refusal here is the belt that does not depend on it.
   */
  static StepResult serviceClients(RunContext context) {
    JsonNode claims =
        context.answer(CLAIMS_IDP_CLIENTS).map(answer -> answer.get("claims")).orElse(null);
    if (claims == null || !claims.isArray() || claims.isEmpty()) {
      return StepResult.refused(
          CLAIMS_IDP_CLIENTS
              + " answered no claims; an empty claim set is never sent, because it would leave"
              + " every service client unclaimed");
    }
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.set("claims", claims);
    return StepResult.of(
        context.peers().post(PeerTarget.IDP, "/idp/api/gc/service-clients", body.toString()),
        answer -> GcSummaries.idpServiceClients(answer.json()));
  }

  /** {@code {"dryRun":…, "minAge":"PT24H"}} */
  private String volumesBody(RunContext context) {
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.put("minAge", config.volumeMinAge().toString());
    return body.toString();
  }

  /**
   * {@code {"dryRun":…, "keepStorageBytes":…, "builderKeepStorageBytes":…}}
   *
   * <p><b>Two budgets, because the two caches are not the same kind of thing.</b>
   * {@code keepStorageBytes} is the host builder's — the cache every CI build warms and re-reads.
   * {@code builderKeepStorageBytes} is a {@code buildx_buildkit_*} container's, which is warmed once
   * while a machine is bootstrapped and then unread until the next bootstrap.
   *
   * <p>One number for both is what left a 13.7 GB bootstrap builder untouched every night: it was
   * smaller than the host budget, so a shared keep-storage never reached it. qits-containers falls
   * back to {@code keepStorageBytes} when the second field is absent, so sending it is safe against
   * a peer that predates it.
   */
  private String buildCacheBody(RunContext context) {
    ObjectNode body = JSON.createObjectNode();
    body.put("dryRun", context.dryRun());
    body.put("keepStorageBytes", config.buildCacheKeepBytes());
    body.put("builderKeepStorageBytes", config.builderCacheKeepBytes());
    return body.toString();
  }
}
