package eu.wohlben.qits.orchestrator.stories.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import eu.wohlben.qits.servicemock.idp.MockIdp;
import eu.wohlben.qits.userflows.Labels;
import eu.wohlben.qits.userflows.NetworkCapture;
import eu.wohlben.qits.userflows.NetworkEdge;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * <b>The eight services a gc run drives, and the ninth it borrows a credential from</b> — one
 * in-JVM stub impersonating all of them, plus the <b>outgoing</b> tap that draws what the launched
 * process asked each one.
 *
 * <h2>Why the far side is the only place this traffic exists</h2>
 *
 * <p>A technical process only SENDS REQUESTS. Everything it does happens on the far side of a
 * socket from this JVM, so a story that wanted to show what a run actually did has exactly one
 * source of evidence: the record each far side keeps of being asked. There are eight of them, and
 * every one is a decision written down in {@code qits-orchestrator-plan.md}:
 *
 * <pre>
 * usage.before / usage.after   qits-containers               GET  /containers/api/gc/usage
 * artifacts.usage.before/after qits-artifacts                GET  /artifacts/api/store/summary
 * pins.deployments             qits-platform-deployments     GET  /deployments/api/pins
 * pins.ci                      qits-ci                       GET  /ci/api/daemon
 * pins.dependencies            qits-platform-maintenance     GET  /maintenance/api/pins
 * pins.images                  qits-configuration            GET  /configuration/api/pins
 * pins.workspaces              qits-workspaces               GET  /workspaces/api/pins
 * pins.projects                qits-projects                 GET  /projects/api/pins
 * artifacts.plan               qits-artifacts                POST /artifacts/api/gc/plan
 * artifacts.sweep              qits-artifacts                POST /artifacts/api/gc/sweep
 * containers.images            qits-containers               POST /containers/api/gc/images
 * containers.volumes           qits-containers               POST /containers/api/gc/volumes
 * containers.build-cache       qits-containers               POST /containers/api/gc/build-cache
 * repos.catalogue              qits-projects                 GET  /projects/api/repositories
 * branches.sweep               qits-workspaces               POST /workspaces/api/gc/branches
 * configuration.entries        qits-configuration            POST /configuration/api/gc/entries
 * tags.sweep                   qits-projects                 POST /projects/api/gc/tags
 * </pre>
 *
 * <p><b>One process impersonates all of them, and the diagram is drawn from the PATH.</b> The eight
 * are eight urls in this service's configuration and would be eight hosts on a platform; here they
 * are eight contexts on one stub, and {@link #peer} maps a path prefix onto the name a reader knows
 * the peer by. Nothing about the evidence changes — direction, method, path and status are what an
 * edge is — and eight servers would only be seven more ports to park.
 *
 * <p><b>The ninth is qits-platform-idp</b>, {@code POST /idp/token}: the outbound half of this
 * service's identity, which the one named oidc client, {@code qits}, presents to every peer alike
 * (service-client-identity-plan.md, C4 — one client replaced eight, one per peer, before it). It
 * draws as the same node {@link MockIdp} does, because it is the same component — the mock serves
 * the inbound half (the JWKS this service validates callers against) and this stub serves the
 * outbound half.
 *
 * <h2>Stateless, with one deliberate exception</h2>
 *
 * <p>Every answer is a pure function of the request: the path decides the document and, for the
 * three bodies that carry {@code dryRun}, the flag is echoed back the way the real peer would judge
 * by it. There is nothing to arm and nothing to reset, so each story class is runnable on its own.
 *
 * <p>The exception is {@link #refuse}, and it exists because <b>no story-controlled value reaches a
 * peer path here</b>. In a repository whose peers are addressed per subject ({@code
 * …/applications/story-misconfigured/resolved}) a refusal can be keyed on the name in the url, and
 * being unreadable is then what that name MEANS. A gc run's seventeen paths are fixed by {@code
 * GcProcess.steps()} and identical in every run, so "this peer is down tonight" cannot be spelled
 * as a path. It is spelled as a file instead — written by the one story about a broken peer, in a
 * {@code try}/{@code finally} that always clears it, wiped again when the stub starts, and read
 * fresh on every request. A file rather than a static field because the stub is started by the
 * <b>test profile</b>, which a launched-artifact run instantiates in a different classloader from
 * the one a story method lives in: two copies of this class, one file.
 *
 * <h2>The recording, and the tap</h2>
 *
 * <p>Every answered request is appended to a file as {@code METHOD PATH STATUS} — before the
 * response is written, so a line is on disk by the time its effect is observable — and {@link
 * #install()} takes the end of that file as a <b>floor</b>. There is genuinely nothing here from
 * before the first story: a peer is called only from inside a run, and the oidc clients ship {@code
 * early-tokens-acquisition=false}, so the floor is a belt rather than a correction.
 *
 * <p><b>Nothing needs to be awaited.</b> A story drives a run to completion (the executor writes
 * {@code finished_at} only after the last step's row), and the stub records a line before it
 * answers — so every line a run produced is on disk before the poll that saw the run close.
 *
 * <h2>The credential is minted ONCE, and that is why one diagram carries it</h2>
 *
 * <p>quarkus-oidc-client caches the token it acquires and re-mints only when it expires, so the
 * {@code POST /idp/token} arrow belongs to the <b>first run of the whole catalogue</b> and to no
 * other. That is a real property of this service rather than an artefact here: {@code PeerTokens}
 * holds one {@code TokensHelper} for the one named client precisely so that a nineteen-step run is
 * not nineteen token requests.
 *
 * <p>What this stand-in chooses is only that the horizon is the whole run: the token says {@code
 * expires_in: 3600}, so the mint lands in exactly one story and every other story's edge count is
 * stable. Measured elsewhere the other way — at {@code expires_in: 1} (and at {@code 0}, which
 * quarkus reads as the same thing) the credential outlives some runs and not others, and the arrow
 * appears in whichever diagram happened to be more than a second after the last. An edge that comes
 * and goes with the clock is a {@code networkHash} that never settles.
 *
 * <p>Before service-client-identity-plan.md's C4 this was eight clients minting eight tokens on the
 * first run, drawn as ONE arrow because an edge is {@code (kind, from, to, label)} and all eight
 * were identical in every one of those four. One named client, {@code qits}, now makes it literally
 * one mint rather than eight collapsed into one label. The corollary to know when running one class
 * alone: {@code stories.collection.GarbageCollectionRunIT} claims that arrow, and any other story
 * class run on its own inherits it and fails its own edge count — loudly, which is the right way for
 * that assumption to break.
 */
public final class StoryPeers {

  // --- how a diagram names each far side ------------------------------------------------------

  /** The component that holds the platform's docker socket: images, volumes and buildkit cache. */
  public static final String CONTAINERS = "qits-containers";

  /** The registry's own GC engine — plan and sweep. */
  public static final String ARTIFACTS = "qits-artifacts";

  /** The daemon-binary pin. */
  public static final String CI = "qits-ci";

  /** The image shas a restart or a rollback would pull. */
  public static final String DEPLOYMENTS = "qits-platform-deployments";

  /** The repository catalogue the branch sweep runs over. */
  public static final String PROJECTS = "qits-projects";

  /** Branch semantics, and the merged-branch sweep that owns them. */
  public static final String WORKSPACES = "qits-workspaces";

  /** The dependency pins: what every repository's main still references in its manifests. */
  public static final String MAINTENANCE = "qits-platform-maintenance";

  /** The configured container image versions the NEXT deploy of a launching service would get. */
  public static final String CONFIGURATION = "qits-configuration";

  /** The identity provider — here as the outbound token endpoint. Same node {@link MockIdp} is. */
  public static final String IDP = MockIdp.SERVICE_NAME;

  /** Every peer, for the negative claims a refusal story makes about all of them at once. */
  public static final List<String> ALL =
      List.of(CONTAINERS, ARTIFACTS, CI, DEPLOYMENTS, PROJECTS, WORKSPACES, MAINTENANCE, CONFIGURATION);

  // --- the paths, exactly as GcProcess spells them --------------------------------------------

  public static final String USAGE_PATH = "/containers/api/gc/usage";
  public static final String STORE_PATH = "/artifacts/api/store/summary";
  public static final String DEPENDENCY_PINS_PATH = "/maintenance/api/pins";
  public static final String IMAGE_PINS_PATH = "/configuration/api/pins";
  public static final String WORKSPACE_LAUNCH_PINS_PATH = "/workspaces/api/pins";
  public static final String PROJECT_LAUNCH_PINS_PATH = "/projects/api/pins";
  public static final String IMAGES_PATH = "/containers/api/gc/images";
  public static final String VOLUMES_PATH = "/containers/api/gc/volumes";
  public static final String BUILD_CACHE_PATH = "/containers/api/gc/build-cache";
  public static final String PINS_PATH = "/deployments/api/pins";
  public static final String DAEMON_PATH = "/ci/api/daemon";
  public static final String PLAN_PATH = "/artifacts/api/gc/plan";
  public static final String SWEEP_PATH = "/artifacts/api/gc/sweep";
  public static final String REPOSITORIES_PATH = "/projects/api/repositories";
  public static final String BRANCHES_PATH = "/workspaces/api/gc/branches";
  public static final String CONFIGURATION_ENTRIES_PATH = "/configuration/api/gc/entries";
  public static final String TAGS_SWEEP_PATH = "/projects/api/gc/tags";
  public static final String TOKEN_PATH = "/idp/token";

  // --- the figures the stories read back out of a summary -------------------------------------

  /** The application every deployment pin in this catalogue names. */
  public static final String PINNED_APPLICATION = "qits-platform-orchestrator";

  /**
   * The sha that application is serving. A long hex run, so {@link Labels} would rewrite it in a
   * label — it never appears in one: it travels in a BODY, which is not hashed.
   */
  public static final String PINNED_SHA = "3f2a91c4e7b8d05612a4c8f9013b7e6d5a4c2b19";

  /** The local tag the image keep-set derives from the pin above — {@code qits/<app>:<sha>}. */
  public static final String PINNED_IMAGE = "qits/" + PINNED_APPLICATION + ":" + PINNED_SHA;

  /** The ci daemon's pinned version, which travels verbatim into the artifacts plan request. */
  public static final String CI_DAEMON_VERSION = "2026.815.120000";

  /**
   * The library version a repository's main pins in its pom — consumption rather than deployment,
   * which is the whole reason the maintenance answer is a pin source of its own: nothing deploys a
   * dependency, so no other peer here can say this version is still referenced.
   */
  public static final String MANIFEST_PIN_VERSION = "2026.903.85122";

  /** The workspace image version the next deploy would configure, from qits-configuration. */
  public static final String CONFIGURED_IMAGE_VERSION = "2026.904.160522";

  /**
   * The version a workspace start would pull TODAY, answered by qits-workspaces out of the config it
   * is actually running with.
   *
   * <p><b>It deliberately lags {@link #CONFIGURED_IMAGE_VERSION}</b>, because that lag is the whole
   * reason the two effective sources exist: a service keeps launching the version it was deployed
   * with until it is redeployed, and until then the configured answer names an image nobody pulls
   * while the one everybody pulls is unpinned. Two different versions in one run is what makes the
   * story readable — a stub that answered the same string twice would prove nothing.
   */
  public static final String LAUNCHED_IMAGE_VERSION = "2026.903.120000";

  /** The repository the branch sweep iterates over — one row of the projects catalogue. */
  public static final String CATALOGUE_REPOSITORY = "qits-platform-orchestrator";

  /** What a refused peer answers, and what a step then records as its error. */
  public static final int REFUSED_STATUS = 503;

  /**
   * How long the disk-usage read takes.
   *
   * <p><b>It is not decoration.</b> A real gc run is minutes of somebody else's pruning, which is
   * the whole reason {@code POST /processes/{kind}/runs} answers 202 and the client polls; a stub
   * answering in microseconds would make "only one run at a time" untestable, because the second
   * request would arrive after the first run had already closed. Half a second on the one call
   * every run begins with is enough for the story to ask the question, and costs four runs two
   * seconds between them.
   */
  private static final long USAGE_LATENCY_MILLIS = 500;

  private static final String PORT_PROPERTY = "qits.test.story-peers.port";

  private static final String SOURCE_ID = "story-peers";

  private static final Path ROOT = Path.of("target", "story-peers");

  /** The recording: one line per answered request, the shape an access log has. */
  private static final Path ACCESS_LOG = ROOT.resolve("access.log");

  /** The one piece of state: which path prefixes answer {@link #REFUSED_STATUS} right now. */
  private static final Path REFUSALS = ROOT.resolve("refusals");

  private static final Object LOCK = new Object();

  private static boolean registered;

  private static int floor;

  private static int harvested;

  private static final List<NetworkEdge> EDGES = new ArrayList<>();

  private StoryPeers() {}

  // --- the server -------------------------------------------------------------------------------

  /**
   * Start the stub once per JVM and park its port, wiping whatever an earlier run left behind.
   * Called from the test profile, which is the only place that knows the urls in time.
   */
  public static synchronized String ensureStarted() {
    String port = System.getProperty(PORT_PROPERTY);
    if (port != null) {
      return baseUrl(Integer.parseInt(port));
    }
    wipe();
    HttpServer server;
    try {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    } catch (IOException e) {
      throw new UncheckedIOException("could not start the story peers stub", e);
    }
    server.createContext("/", StoryPeers::handle);
    server.start();
    System.setProperty(PORT_PROPERTY, String.valueOf(server.getAddress().getPort()));
    return baseUrl(server.getAddress().getPort());
  }

  private static String baseUrl(int port) {
    return "http://localhost:" + port;
  }

  private static void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    String method = exchange.getRequestMethod();
    String request = requestBody(exchange);

    int status;
    String body;
    if (isRefused(path)) {
      status = REFUSED_STATUS;
      body = "{\"message\":\"" + path + " is unavailable\"}";
    } else {
      String answer = answer(method, path, request);
      status = answer == null ? 404 : 200;
      body = answer == null ? "{\"message\":\"no such route\"}" : answer;
    }

    if (USAGE_PATH.equals(path)) {
      sleep();
    }

    // Recorded BEFORE the answer leaves, so a story that observed an effect can rely on the line
    // for it already being on disk. There is nothing to await.
    record(method, path, status);
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  /**
   * What each peer answers — the smallest documents that make every {@code GcSummaries} reader
   * produce a real line, and no larger.
   *
   * <p>The figures are the ones the summariser quotes back, so a story can assert the human line an
   * operator reads without this file computing anything: 43.5 GB of images with 19.3 GB
   * reclaimable, a plan condemning 128 identities, a sweep unlinking 91 blobs. Every one of them is
   * READ out of the answer by qits-platform-orchestrator and never derived, which is the rule the
   * summariser exists to keep.
   */
  private static String answer(String method, String path, String request) {
    boolean dryRun = request != null && request.contains("\"dryRun\":true");
    return switch (path) {
      case USAGE_PATH ->
          "{\"images\":{\"sizeBytes\":43500000000,\"reclaimableBytes\":19300000000},"
              + "\"buildCache\":{\"sizeBytes\":35100000000}}";
      // The registry plane, which the docker read above cannot see: these bytes are rows in
      // qits-artifacts' own store, and a run measured by `docker system df` alone reported a
      // platform that was not growing while 50 GB of registry did.
      case STORE_PATH ->
          "{\"diskTotalBytes\":51200000000,\"ociUnionBytes\":50700000000,"
              + "\"docsBytes\":164200000,\"sbomBytes\":112600000}";
      case DEPENDENCY_PINS_PATH ->
          "{\"repositories\":[{\"name\":\""
              + CATALOGUE_REPOSITORY
              + "\",\"status\":\"OK\"}],"
              + "\"pins\":[{\"ecosystem\":\"maven\",\"name\":\"eu.wohlben.qits:qits-blobstore\","
              + "\"version\":\""
              + MANIFEST_PIN_VERSION
              + "\",\"repository\":\""
              + CATALOGUE_REPOSITORY
              + "\",\"manifestPath\":\"pom.xml\"}]}";
      case IMAGE_PINS_PATH ->
          "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\""
              + CONFIGURED_IMAGE_VERSION
              + "\",\"application\":\"qits-workspaces\","
              + "\"key\":\"env.QITS_WORKSPACE_IMAGE_VERSION\"}]}";
      // The EFFECTIVE pins, and the version is the older one on purpose — see the constant.
      case WORKSPACE_LAUNCH_PINS_PATH ->
          "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\""
              + LAUNCHED_IMAGE_VERSION
              + "\",\"launches\":\"workspace\"}]}";
      case PROJECT_LAUNCH_PINS_PATH ->
          "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\""
              + LAUNCHED_IMAGE_VERSION
              + "\",\"launches\":\"refinement\"}]}";
      case PINS_PATH ->
          "{\"pins\":[{\"applicationName\":\""
              + PINNED_APPLICATION
              + "\",\"shas\":[\""
              + PINNED_SHA
              + "\"]}]}";
      case DAEMON_PATH ->
          "{\"daemonName\":\"qits-ci-daemon\",\"daemonVersion\":\""
              + CI_DAEMON_VERSION
              + "\",\"previousDaemonVersion\":\"2026.814.101010\"}";
      case PLAN_PATH ->
          "{\"summary\":{\"identitiesCondemned\":128,\"reclaimableBytes\":19300000000,"
              + "\"executable\":true}}";
      case SWEEP_PATH -> "{\"sweep\":{\"blobsUnlinked\":91,\"bytesReclaimed\":17800000000}}";
      case IMAGES_PATH ->
          "{\"removed\":[\"qits/story-one:aaa\",\"qits/story-two:bbb\"],"
              + "\"bytesReclaimed\":9400000000,"
              + "\"kept\":[\""
              + PINNED_IMAGE
              + "\",\"qits/build-images/maven-base:latest\"]}";
      case VOLUMES_PATH -> "{\"removed\":[\"orphan-one\"],\"kept\":[\"story-data\"]}";
      case BUILD_CACHE_PATH ->
          "{\"host\":{\"reclaimedBytes\":12600000000},"
              + "\"builders\":[{\"name\":\"buildx_buildkit_story0\",\"reclaimedBytes\":3100000000}]}";
      case REPOSITORIES_PATH ->
          "{\"repositories\":[{\"id\":\"0f1e2d3c-4b5a-4968-8778-695a4b3c2d1e\",\"name\":\""
              + CATALOGUE_REPOSITORY
              + "\",\"mainBranch\":\"main\"}]}";
      case BRANCHES_PATH ->
          "{\"dryRun\":"
              + dryRun
              + ",\"repositoriesExamined\":1,\"branchesExamined\":214,"
              + "\"removed\":[{\"repositoryName\":\""
              + CATALOGUE_REPOSITORY
              + "\",\"branch\":\"old-work\"}],\"errors\":[]}";
      case CONFIGURATION_ENTRIES_PATH ->
          "{\"dryRun\":"
              + dryRun
              + ",\"examined\":1,"
              + "\"removed\":[{\"application\":\""
              + PINNED_APPLICATION
              + "\",\"env\":\"dev\",\"key\":\"env.OLD_FLAG\",\"reason\":\"retired\","
              + "\"lastDeclaredBy\":\""
              + CI_DAEMON_VERSION
              + "\"}],"
              + "\"kept\":{\"unpinned\":0,\"undeclaredPinnedVersion\":0,\"pinned\":1,"
              + "\"inFlight\":0,\"neverDeclared\":0,\"staged\":0},\"errors\":[]}";
      case TAGS_SWEEP_PATH ->
          "{\"dryRun\":"
              + dryRun
              + ",\"repositories\":1,\"examined\":1,"
              + "\"deleted\":[{\"repository\":\""
              + CATALOGUE_REPOSITORY
              + "\",\"tag\":\"2026.814.090000\",\"host\":true,\"twin\":true}],"
              + "\"kept\":{\"newest\":0,\"pinnedVersion\":1,\"gitlink\":0,\"inFlight\":0,"
              + "\"young\":0},\"errors\":[]}";
      // An hour, so the mints land in exactly one story of the run — see the class javadoc.
      case TOKEN_PATH ->
          "POST".equals(method)
              ? "{\"access_token\":\"story-orchestrator-machine-token\","
                  + "\"token_type\":\"Bearer\",\"expires_in\":3600}"
              : null;
      default -> null;
    };
  }

  private static String requestBody(HttpExchange exchange) {
    try {
      return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException unreadable) {
      return null;
    }
  }

  private static void sleep() {
    try {
      Thread.sleep(USAGE_LATENCY_MILLIS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  // --- the one piece of state -------------------------------------------------------------------

  /**
   * Make every path starting with {@code prefix} answer {@link #REFUSED_STATUS} until {@link
   * #answerNormally()} is called. One prefix at a time — a second call replaces the first, because
   * a story about a broken peer is about ONE broken peer and the rest of the platform answering.
   *
   * <p><b>Always in a {@code try}/{@code finally}.</b> A refusal that outlived its story would be a
   * broken peer in somebody else's diagram, and the two would look exactly alike.
   */
  public static void refuse(String prefix) {
    write(REFUSALS, prefix + "\n");
  }

  /** Clear every armed refusal. Idempotent, and safe to call when nothing was armed. */
  public static void answerNormally() {
    try {
      Files.deleteIfExists(REFUSALS);
    } catch (IOException e) {
      throw new UncheckedIOException("could not clear " + REFUSALS, e);
    }
  }

  private static boolean isRefused(String path) {
    if (!Files.isRegularFile(REFUSALS)) {
      return false;
    }
    String armed;
    try {
      armed = Files.readString(REFUSALS, StandardCharsets.UTF_8);
    } catch (IOException unreadable) {
      return false;
    }
    for (String prefix : armed.split("\n")) {
      if (!prefix.isBlank() && path.startsWith(prefix.strip())) {
        return true;
      }
    }
    return false;
  }

  // --- what a story class calls -------------------------------------------------------------------

  /**
   * Register the tap once per JVM, taking the current end of the recording as the floor. Called
   * from every story class's {@code @BeforeAll}; whichever runs first bounds what any story sees.
   */
  public static void install() {
    synchronized (LOCK) {
      if (registered) {
        return;
      }
      floor = allLines().size();
      harvested = 0;
      NetworkCapture.source(SOURCE_ID, StoryPeers::edges);
      registered = true;
    }
  }

  /** The label an answered peer call renders as — what an assertion has to spell. */
  public static String label(String method, String path, int status) {
    return Labels.scrub(method + " " + path + " -> " + status);
  }

  /** {@code GET <path> -> 200}. */
  public static String read(String path) {
    return label("GET", path, 200);
  }

  /** {@code POST <path> -> 200}. */
  public static String written(String path) {
    return label("POST", path, 200);
  }

  // --- the source ---------------------------------------------------------------------------------

  private static List<NetworkEdge> edges() {
    synchronized (LOCK) {
      harvest();
      return List.copyOf(EDGES);
    }
  }

  private static void harvest() {
    List<String> lines = readLines();
    if (harvested > lines.size()) {
      harvested = 0;
      floor = 0;
      lines = readLines();
    }
    for (String line : lines.subList(harvested, lines.size())) {
      edge(line).ifPresent(EDGES::add);
    }
    harvested = lines.size();
  }

  /** One recorded line as an edge, attributed to the peer whose path prefix it carries. */
  private static Optional<NetworkEdge> edge(String line) {
    // "METHOD PATH STATUS" — three fields, no quoting, and a path carries no raw space.
    String[] fields = line.strip().split(" ");
    if (fields.length != 3 || !fields[1].startsWith("/")) {
      return Optional.empty();
    }
    String peer = peer(fields[1]);
    if (peer == null) {
      return Optional.empty();
    }
    return Optional.of(
        NetworkEdge.http(
            StoryTarget.SERVICE,
            peer,
            Labels.scrub(fields[0] + " " + fields[1] + " -> " + fields[2])));
  }

  /** Which peer a path belongs to — the whole of how one stub draws as seven. */
  private static String peer(String path) {
    if (path.startsWith("/containers/")) {
      return CONTAINERS;
    }
    if (path.startsWith("/artifacts/")) {
      return ARTIFACTS;
    }
    if (path.startsWith("/ci/")) {
      return CI;
    }
    if (path.startsWith("/deployments/")) {
      return DEPLOYMENTS;
    }
    if (path.startsWith("/projects/")) {
      return PROJECTS;
    }
    if (path.startsWith("/workspaces/")) {
      return WORKSPACES;
    }
    if (path.startsWith("/maintenance/")) {
      return MAINTENANCE;
    }
    if (path.startsWith("/configuration/")) {
      return CONFIGURATION;
    }
    if (path.startsWith("/idp/")) {
      return IDP;
    }
    return null;
  }

  /** Everything recorded since the floor — i.e. everything a story could own. */
  private static List<String> readLines() {
    List<String> all = allLines();
    return floor >= all.size() ? List.of() : all.subList(floor, all.size());
  }

  /**
   * The recording's complete lines. A missing file is an empty recording rather than a failure, and
   * an <b>unterminated tail is dropped</b>: the server appends while this reads, and half a line
   * would shape half an edge. The next harvest sees it whole.
   */
  private static List<String> allLines() {
    if (!Files.isRegularFile(ACCESS_LOG)) {
      return List.of();
    }
    String text;
    try {
      text = Files.readString(ACCESS_LOG, StandardCharsets.UTF_8);
    } catch (IOException unreadable) {
      return List.of();
    }
    int lastComplete = text.lastIndexOf('\n');
    if (lastComplete < 0) {
      return List.of();
    }
    return List.of(text.substring(0, lastComplete).split("\n"));
  }

  private static synchronized void record(String method, String path, int status) {
    try {
      Files.createDirectories(ROOT);
      Files.writeString(
          ACCESS_LOG,
          method + " " + path + " " + status + "\n",
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException ignored) {
      // A recording that cannot be written costs the diagram an arrow; it must not cost the
      // launched process its answer, which is what a run is actually waiting for.
    }
  }

  private static void write(Path file, String content) {
    try {
      Files.createDirectories(ROOT);
      Files.writeString(file, content, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("could not write " + file, e);
    }
  }

  private static void wipe() {
    try {
      Files.deleteIfExists(ACCESS_LOG);
      Files.deleteIfExists(REFUSALS);
    } catch (IOException e) {
      throw new UncheckedIOException("could not clear " + ROOT, e);
    }
  }
}
