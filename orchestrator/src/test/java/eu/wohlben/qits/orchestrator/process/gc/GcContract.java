package eu.wohlben.qits.orchestrator.process.gc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.pact.consumer.ConsumerPact;
import eu.wohlben.qits.pact.consumer.GoldenInteraction;
import eu.wohlben.qits.pact.consumer.GoldenMasters;
import eu.wohlben.qits.pact.consumer.GoldenMasters.Operation;
import eu.wohlben.qits.pact.consumer.ProviderStateMissing;
import eu.wohlben.qits.pact.consumer.Trigger;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * <b>What the gc process asks its peers, and what it reads from each answer</b> (ticket qits-1149,
 * epic qits-546): one {@link ConsumerPact} per peer, built with qits-pact-consumer from that peer's
 * published golden masters, and the table {@code GcConsumerPactTest}, {@code IdpTokenMintPactTest}
 * and {@code GcCallCoverageTest} are all built from. The qits-idp pact also carries the two reads
 * the service's quarkus-oidc tenant makes at startup ({@link #TENANT_READS}, qits-708).
 *
 * <p><b>One row per call.</b> Every call is a step of {@link GcProcess}; the trigger named is the
 * nightly {@code GcSchedule.onSchedule}. A manual {@code POST /processes/gc/runs} and the {@code
 * DeploymentActive} trigger run the identical steps, so they would only repeat each interaction.
 * {@code usage.before}/{@code usage.after} and the two {@code artifacts.usage.*} steps are one call
 * each, made twice in a run: one row each.
 *
 * <p><b>{@code consumes} is what this service reads</b>: the fields {@link GcSummaries} turns into
 * the step's line, and the fields a later step's request is built from ({@code pins[].shas} for the
 * image keep-set, {@code repositories[].id} for the branch sweep, {@code claims[]} for qits-idp). A
 * pin answer is also forwarded WHOLE to qits-artifacts and qits-projects: that
 * part is a contract between the pin source and its reader, not this service's, so it is not bound
 * here.
 *
 * <p><b>The request is the provider's recording</b>, values exact. A row's {@link Arrange} is what
 * makes the step send exactly that: the earlier steps' answers the body is built from, taken from
 * the recorded request, and — where the recording was made under another setting than the one
 * shipped — that setting.
 */
final class GcContract {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** This repository, as every pact names its consumer. */
  static final String CONSUMER = "qits-orchestrator-service";

  /** The trigger every gc row is reached from. */
  static final Trigger NIGHTLY = Trigger.schedule("GcSchedule.onSchedule");

  /** What a row needs in place before its step sends the recorded request. */
  @FunctionalInterface
  interface Arrange {
    /**
     * @param recorded the recorded request body, every {@code {param}} its example; null for a GET
     */
    void apply(JsonNode recorded, GcConfig config, RunContext context);
  }

  private static final Arrange NOTHING = (recorded, config, context) -> {};

  /**
   * One call.
   *
   * @param target the {@link PeerTarget} the step calls
   * @param stepId the {@link GcProcess} step that makes the call
   */
  record Row(String target, String stepId, GoldenInteraction interaction, Arrange arrange) {}

  // --- the peers ---------------------------------------------------------------------------------

  static final GoldenMasters CONTAINERS =
      GoldenMasters.of("qits-containers-service", "qits-containers");
  static final GoldenMasters ARTIFACTS = GoldenMasters.of("qits-artifacts-service", "qits-artifacts");
  static final GoldenMasters DEPLOYMENTS =
      GoldenMasters.of("qits-deployments-service", "qits-deployments");
  static final GoldenMasters CI = GoldenMasters.of("qits-ci-service", "qits-ci");
  static final GoldenMasters MAINTENANCE =
      GoldenMasters.of("qits-maintenance-service", "qits-maintenance");
  static final GoldenMasters WORKSPACES =
      GoldenMasters.of("qits-workspaces-service", "qits-workspaces");
  static final GoldenMasters PROJECTS = GoldenMasters.of("qits-projects-service", "qits-projects");
  static final GoldenMasters IDP = GoldenMasters.of("qits-idp-service", "qits-idp");

  /** The peer each {@link PeerTarget} is. */
  static final Map<String, GoldenMasters> PEERS =
      Map.of(
          PeerTarget.CONTAINERS, CONTAINERS,
          PeerTarget.ARTIFACTS, ARTIFACTS,
          PeerTarget.DEPLOYMENTS, DEPLOYMENTS,
          PeerTarget.CI, CI,
          PeerTarget.MAINTENANCE, MAINTENANCE,
          PeerTarget.WORKSPACES, WORKSPACES,
          PeerTarget.PROJECTS, PROJECTS,
          PeerTarget.IDP, IDP);

  // --- the rows ----------------------------------------------------------------------------------

  private static final String RECLAIMABLE_HOST = "a host with reclaimable storage";
  private static final String UNPINNED_REGISTRY = "a registry with unpinned identities";

  static final List<Row> ROWS =
      List.of(
          // qits-containers-service
          row(PeerTarget.CONTAINERS, GcProcess.USAGE_BEFORE, RECLAIMABLE_HOST, "getGcUsage", NOTHING,
              "images.sizeBytes", "images.reclaimableBytes", "buildCache.sizeBytes"),
          row(PeerTarget.CONTAINERS, GcProcess.CONTAINERS_IMAGES, RECLAIMABLE_HOST, "collectImages",
              GcContract::deploymentPinsFromKeep, "removed[]", "bytesReclaimed", "kept[]"),
          row(PeerTarget.CONTAINERS, GcProcess.CONTAINERS_VOLUMES, RECLAIMABLE_HOST,
              "collectVolumes", NOTHING, "removed[]", "kept[]"),
          row(PeerTarget.CONTAINERS, GcProcess.CONTAINERS_BUILD_CACHE, RECLAIMABLE_HOST,
              "pruneBuildCache", NOTHING, "host.reclaimedBytes", "builders[].reclaimedBytes"),
          // qits-artifacts-service
          row(PeerTarget.ARTIFACTS, GcProcess.ARTIFACTS_USAGE_BEFORE, "a registry store with content",
              "getStoreSummary", NOTHING,
              "diskTotalBytes", "ociUnionBytes", "docsBytes", "sbomBytes"),
          row(PeerTarget.ARTIFACTS, GcProcess.ARTIFACTS_PLAN, UNPINNED_REGISTRY,
              "planGcWithSuppliedPins", GcContract::pinsFromBody,
              "summary.identitiesCondemned", "summary.reclaimable", "summary.reclaimableBytes",
              "summary.executable"),
          row(PeerTarget.ARTIFACTS, GcProcess.ARTIFACTS_SWEEP, UNPINNED_REGISTRY, "sweepGc",
              GcContract::pinsFromBody, "sweep.blobsUnlinked", "sweep.bytesReclaimed"),
          // The fail-closed abort: a pin source the sweep could not file. {@code aborted} is the
          // only field GcSummaries#artifactsSweep reads once it is set, sweep section or not.
          row(PeerTarget.ARTIFACTS, GcProcess.ARTIFACTS_SWEEP,
              "a registry handed a pin it cannot file", "sweepGc", GcContract::pinsFromBody,
              "aborted"),
          // qits-deployments-service
          row(PeerTarget.DEPLOYMENTS, GcProcess.PINS_DEPLOYMENTS,
              "an application serving with a rollback version", "listPins", NOTHING,
              "pins[].applicationName", "pins[].shas"),
          row(PeerTarget.DEPLOYMENTS, GcProcess.CLAIMS_IDP_CLIENTS,
              "an application holding a service client", "listIdpClientClaims", NOTHING,
              "claims[].clientId", "claims[].applicationName"),
          // qits-ci-service
          row(PeerTarget.CI, GcProcess.PINS_CI, "a pinned daemon", "getDaemonPin", NOTHING,
              "daemonName", "daemonVersion", "previousDaemonVersion"),
          // qits-maintenance-service
          row(PeerTarget.MAINTENANCE, GcProcess.PINS_DEPENDENCIES, "manifests that pin artifacts",
              "listDependencyPins", NOTHING, "pins[]", "repositories[]"),
          // qits-workspaces-service. anyLength("pins"): this service reads no field of a pin, only
          // the array's presence (GcSummaries#workspaceLaunchPins counts it for the step's line, and
          // the whole answer rides to qits-artifacts verbatim — see pinsFromBody). Pinning the count
          // would bind this consumer to how many image kinds qits-workspaces happens to launch today
          // — qits-1532 drops its runner row from this very answer — which is not a contract either
          // side means to hold.
          new Row(
              PeerTarget.WORKSPACES,
              GcProcess.PINS_WORKSPACES,
              GoldenInteraction.of(NIGHTLY, "a configured workspace image", "listLaunchPins")
                  .consumes("pins[]")
                  .anyLength("pins"),
              NOTHING),
          row(PeerTarget.WORKSPACES, GcProcess.BRANCHES_SWEEP, "repositories with merged branches",
              "sweepBranches", GcContract::catalogueFromBody,
              "removed[]", "branchesExamined", "repositoriesExamined", "errors[]", "dryRun"),
          // qits-projects-service
          row(PeerTarget.PROJECTS, GcProcess.PINS_PROJECTS, "an agent launch image in use",
              "listLaunchPins", NOTHING, "pins[]"),
          row(PeerTarget.PROJECTS, GcProcess.REPOS_CATALOGUE, "a project with 3 repositories",
              "listRepositories", NOTHING,
              "repositories[].id", "repositories[].name", "repositories[].mainBranch"),
          row(PeerTarget.PROJECTS, GcProcess.TAGS_SWEEP, "repositories with decommissionable tags",
              "collectTags", GcContract::pinsFromBody,
              "deleted[].host", "deleted[].twin", "repositories", "errors[]", "dryRun",
              "kept.newest", "kept.pinnedVersion", "kept.gitlink", "kept.inFlight", "kept.young"),
          // qits-idp-service. The GC door is called, like every peer, with this service's own
          // BEARER (PeerClient sends `Authorization: Bearer <PeerTokens' token>`), which that door
          // alone accepts for a qits:system service client: the state is the one judged by the
          // caller's bearer, its `authorization` bound as a provider-state generator, and the test
          // hands the token it carries to PeerTokens (#bearer).
          new Row(
              PeerTarget.IDP,
              GcProcess.IDP_SERVICE_CLIENTS,
              GoldenInteraction.of(
                      NIGHTLY,
                      "a service client nothing claims, calling with its bearer",
                      "collectServiceClients")
                  .header("Authorization", "{authorization}")
                  .consumes(
                      "dryRun", "removed[].clientId", "kept[]", "keptCounts.claimed",
                      "keptCounts.grace", "keptCounts.caller"),
              GcContract::claimsFromBody));

  /**
   * The client-credentials mint {@code PeerTokens} makes through quarkus-oidc-client's {@code
   * qits} client before a peer call: Basic client auth and {@code
   * grant_type=client_credentials&audience=qits-platform}. Not a gc step, so not in {@link #ROWS};
   * {@code IdpTokenMintPactTest} runs it.
   *
   * <p><b>The Basic pair is the state's caller.</b> qits-idp records the mint with the {@code
   * Authorization} its state names as {@code {authorization}}; the row sends that header as a
   * provider-state generator, and the test configures the oidc client with the pair the header
   * encodes, so what quarkus-oidc-client puts on the wire is exactly the credential the provider's
   * state honours. Without it the idp's honest answer is 401.
   */
  static final String A_PEER_CALLER = "a service client calling its peers";

  static final GoldenInteraction TOKEN_MINT =
      GoldenInteraction.of(Trigger.operation("PeerTokens.token"), A_PEER_CALLER, "issueToken")
          .header("Authorization", "{authorization}")
          .consumes("access_token");

  // --- the quarkus-oidc tenant's two startup reads (qits-708) -------------------------------------

  /**
   * The qits-idp state that records the discovery document and the JWKS. The rows below are not gc
   * steps and no class of this source tree makes their calls: the quarkus-oidc TENANT in {@code
   * service/} makes them at startup once the machine gate turns it on — first the discovery document
   * read from {@code quarkus.oidc.auth-server-url}, then the JWKS at the {@code jwks_uri} it names.
   * They belong in this service's one qits-idp pact all the same, and {@code GcConsumerPactTest}
   * reads each answer as quarkus-oidc reads it; {@code GcCallCoverageTest} holds them to the shipped
   * tenant configuration in {@code service/src/main/resources/application.properties}.
   */
  static final String IDP_SIGNING_KEY_STATE = "a published signing key";

  /** The tenant's startup reads, as qits-idp's other consumers trigger them. */
  static final Trigger STARTUP = Trigger.event("StartupEvent");

  /**
   * The discovery document: the issuer the tenant checks every bearer's {@code iss} against, and
   * where the keys are. The issuer is bound BY VALUE ({@code exact}): an idp answering another
   * issuer refuses every token this service accepts, so its type alone is not the contract.
   */
  static final GoldenInteraction IDP_DISCOVERY =
      GoldenInteraction.of(STARTUP, IDP_SIGNING_KEY_STATE, "getOpenIdConfiguration")
          .consumes("issuer", "jwks_uri", "token_endpoint")
          .exact("issuer");

  /** The JWKS: each key with what quarkus-oidc needs to build it and to pick it by {@code kid}. */
  static final GoldenInteraction IDP_JWKS =
      GoldenInteraction.of(STARTUP, IDP_SIGNING_KEY_STATE, "getJwks")
          .consumes("keys[].kid", "keys[].kty", "keys[].n", "keys[].e", "keys[].alg", "keys[].use");

  /** Both tenant reads, in the order the tenant makes them. */
  static final List<GoldenInteraction> TENANT_READS = List.of(IDP_DISCOVERY, IDP_JWKS);

  // --- the artifacts cleanup doors (qits-2138) ---------------------------------------------------

  /**
   * The qits-artifacts state the three supplied-pins cleanup operations are recorded in. Its params
   * name the repository the plan and the sweep address ({@code repository}).
   */
  static final String CLEANUP_STATE = "a repository with unpinned identities";

  /**
   * One call an artifacts cleanup door makes ({@link ArtifactsCleanup}): the door's own operation as
   * the trigger, qits-artifacts' supplied-pins operation as the row, and how the door is driven —
   * with the state's params, for the {@code {repository}} the path names.
   *
   * <p><b>Not a gc step, so not in {@link #ROWS}</b>, and {@code GcCallCoverageTest}'s gc pass knows
   * nothing of it; {@link #pact} adds these rows to the qits-artifacts pact beside the gc rows.
   *
   * <p><b>What a door reads is almost nothing</b>, and {@code consumes} says exactly that. The
   * listing's and the plan's answers go back to the caller as the text qits-artifacts sent — that
   * shape is qits-artifacts' contract with its own UI, the same reasoning that leaves a forwarded pin
   * answer unbound — so those rows bind the request and the status alone. The sweep's receipt is read
   * for the one INFO line a sweep writes, exactly as {@link GcSummaries#artifactsSweep} reads a gc
   * run's: {@code sweep.blobsUnlinked} and {@code sweep.bytesReclaimed}.
   *
   * <p><b>Pending while the state is not in the pinned golden masters.</b> qits-artifacts recorded
   * these three operations after the version {@code qits.artifacts-golden-masters.version} pins, so
   * {@link ConsumerPact#recorded()} leaves them out of the pact file and {@code GcConsumerPactTest}
   * reports each one skipped with what it needs. The pin bump that brings them in is what turns them
   * on: from then the committed pact file fails its comparison until it is rewritten with {@code
   * -Dgolden.update=true}.
   */
  record CleanupRow(
      String door,
      GoldenInteraction interaction,
      java.util.function.BiFunction<ArtifactsCleanup, Map<String, String>, PeerExchange> call) {}

  static final List<CleanupRow> CLEANUP =
      List.of(
          new CleanupRow(
              "listArtifactsCleanupRepositories",
              GoldenInteraction.of(
                  Trigger.operation("listArtifactsCleanupRepositories"),
                  CLEANUP_STATE,
                  "planGcRepositoriesWithSuppliedPins"),
              (cleanup, params) -> cleanup.repositories()),
          new CleanupRow(
              "planArtifactsCleanupRepository",
              GoldenInteraction.of(
                  Trigger.operation("planArtifactsCleanupRepository"),
                  CLEANUP_STATE,
                  "planGcRepositoryWithSuppliedPins"),
              (cleanup, params) -> cleanup.plan(params.get("repository"))),
          new CleanupRow(
              "sweepArtifactsCleanupRepository",
              GoldenInteraction.of(
                      Trigger.operation("sweepArtifactsCleanupRepository"),
                      CLEANUP_STATE,
                      "sweepGcRepository")
                  .consumes("sweep.blobsUnlinked", "sweep.bytesReclaimed"),
              (cleanup, params) -> cleanup.sweep(params.get("repository"), "a contract test")));

  /** Whether the pinned qits-artifacts golden masters record {@code row}'s operation. */
  static boolean recorded(CleanupRow row) {
    return ARTIFACTS.recorded(row.interaction().state(), row.interaction().operationId());
  }

  /**
   * {@code row}'s door, outside CDI and as shipped, with the five pins answering the members of the
   * recorded request body and {@code artifacts} answering the call: the pins it reads are exactly
   * the ones the provider recorded the request with, so the request it sends is the recorded one.
   * No gc run is ever active here, so the sweep's exclusion always admits it.
   */
  static PeerExchange runCleanup(CleanupRow row, PeerClient artifacts) {
    Operation op = ARTIFACTS.operation(row.interaction().state(), row.interaction().operationId());
    FakePeers peers = ContractPeers.fakeExcept(PeerTarget.ARTIFACTS, artifacts);
    JsonNode pins = op.body().path("pins");
    for (GcPins.Source source : GcPins.Source.values()) {
      JsonNode member = pins.get(source.member());
      if (member != null) {
        peers.answer(source.path(), FakePeers.Scripted.ok(member.toString()));
      }
    }
    ArtifactsCleanup cleanup = new ArtifactsCleanup();
    cleanup.config = shippedConfig();
    cleanup.peers = peers;
    cleanup.sleeper = duration -> {};
    cleanup.exclusion = new GcExclusion();
    cleanup.exclusion.runs =
        new eu.wohlben.qits.orchestrator.persistence.RunStore() {
          @Override
          public Optional<UUID> live(String kind, java.time.Instant now) {
            return Optional.empty();
          }
        };
    PeerExchange exchange = row.call().apply(cleanup, op.params());
    for (GcPins.Source source : GcPins.Source.values()) {
      if (peers.callsTo(source.path()) != 1) {
        throw new AssertionError(row.door() + " read " + source.path() + " "
            + peers.callsTo(source.path()) + " times");
      }
    }
    return exchange;
  }

  // --- the pending rows ----------------------------------------------------------------------------

  /**
   * A row waiting on a provider state that is not on the classpath yet — the same reason {@link
   * GoldenMasters#operation} would throw {@link ProviderStateMissing}, raised here instead of from
   * inside a running pact so the row can be reported as skipped rather than erroring the whole
   * suite. Not in {@link #ROWS}: {@link #pact} never embeds it, and {@code GcCallCoverageTest}'s
   * guard cannot resolve its request from a recording, so a pending row names its call itself
   * ({@code method} and {@code path}, as the step sends it) and the guard counts that instead. Once
   * the provider publishes the state, move the row into {@link #ROWS} with a real {@code consumes}
   * list built the same way every live row's is.
   *
   * @param needs what the provider's state must carry, beyond the state existing
   */
  record Pending(
      String target, String stepId, String state, String operationId, String method, String path,
      List<String> consumes, String needs) {
    String reason() {
      return ProviderStateMissing.message(PEERS.get(target).repository(), state, operationId)
          + " — needs: " + needs;
    }
  }

  /**
   * None: every call the gc process makes binds a recorded state. The mechanism stays for the next
   * row a provider has not recorded yet.
   */
  static final List<Pending> PENDING = List.of();

  private static Row row(
      String target, String stepId, String state, String operationId, Arrange arrange,
      String... consumes) {
    return new Row(
        target,
        stepId,
        GoldenInteraction.of(NIGHTLY, state, operationId).consumes(consumes),
        arrange);
  }

  /** The pact against each peer, its rows in table order (the pact file sorts). */
  static ConsumerPact pact(String target) {
    List<GoldenInteraction> rows = new ArrayList<>();
    for (Row row : ROWS) {
      if (row.target().equals(target)) {
        rows.add(row.interaction());
      }
    }
    if (target.equals(PeerTarget.ARTIFACTS)) {
      // Left out of the file while the pinned golden masters do not record them (ConsumerPact).
      CLEANUP.forEach(row -> rows.add(row.interaction()));
    }
    if (target.equals(PeerTarget.IDP)) {
      rows.add(TOKEN_MINT);
      rows.addAll(TENANT_READS);
    }
    return ConsumerPact.of(CONSUMER, PEERS.get(target), rows);
  }

  static GoldenMasters masters(Row row) {
    return PEERS.get(row.target());
  }

  static Operation recorded(Row row) {
    return masters(row).operation(row.interaction().state(), row.interaction().operationId());
  }

  /**
   * A stand-in for a row whose recorded interaction pins no {@code Authorization} value: {@code
   * PeerClient} now refuses to send any call with no bearer at all (qits-543), but the pact mock
   * server never asserted this header's value for those rows, so any non-blank bearer satisfies it.
   */
  private static final String DEFAULT_BEARER = "a-contract-test-bearer";

  /**
   * The token {@code PeerTokens} must hand {@code PeerClient} for {@code row}: the {@code Bearer}
   * the state's {@code authorization} param carries, where the row pins that header;
   * {@link #DEFAULT_BEARER} otherwise — a bearer is mandatory on every call now, pinned or not.
   */
  static Optional<String> bearer(Row row) {
    if (!row.interaction().requestHeaders().containsKey("Authorization")) {
      return Optional.of(DEFAULT_BEARER);
    }
    String authorization =
        masters(row).params(row.interaction().state()).get("authorization");
    if (authorization == null || !authorization.startsWith("Bearer ")) {
      throw new IllegalStateException(
          row.interaction().description() + ": the state's authorization is no Bearer");
    }
    return Optional.of(authorization.substring("Bearer ".length()));
  }

  // --- what a row arranges -----------------------------------------------------------------------

  /** {@code keep: ["qits/<application>:<sha>", …]} back into the deployment pins it was built from. */
  private static void deploymentPinsFromKeep(JsonNode recorded, GcConfig config, RunContext context) {
    ObjectNode answer = JSON.createObjectNode();
    ArrayNode pins = answer.putArray("pins");
    for (JsonNode keep : recorded.path("keep")) {
      String image = keep.asText().substring("qits/".length());
      int colon = image.lastIndexOf(':');
      ObjectNode pin = pins.addObject();
      pin.put("applicationName", image.substring(0, colon));
      pin.putArray("shas").add(image.substring(colon + 1));
    }
    seed(context, GcProcess.PINS_DEPLOYMENTS, answer);
  }

  /** {@code pins: {deployments, ciDaemon, dependencies, workspaceLaunches, projectLaunches}}. */
  private static void pinsFromBody(JsonNode recorded, GcConfig config, RunContext context) {
    JsonNode pins = recorded.path("pins");
    seedIfPresent(context, GcProcess.PINS_DEPLOYMENTS, pins.get("deployments"));
    seedIfPresent(context, GcProcess.PINS_CI, pins.get("ciDaemon"));
    seedIfPresent(context, GcProcess.PINS_DEPENDENCIES, pins.get("dependencies"));
    seedIfPresent(context, GcProcess.PINS_WORKSPACES, pins.get("workspaceLaunches"));
    seedIfPresent(context, GcProcess.PINS_PROJECTS, pins.get("projectLaunches"));
  }

  /**
   * {@code repositories} is the catalogue the step read; {@code keepPrefixes} is the recording's
   * setting, which is not the shipped one ({@code environment/,release/}).
   */
  private static void catalogueFromBody(JsonNode recorded, GcConfig config, RunContext context) {
    ObjectNode catalogue = JSON.createObjectNode();
    catalogue.set("repositories", recorded.path("repositories").deepCopy());
    seed(context, GcProcess.REPOS_CATALOGUE, catalogue);
    List<String> prefixes = new ArrayList<>();
    recorded.path("keepPrefixes").forEach(prefix -> prefixes.add(prefix.asText()));
    config.branchKeepPrefixes = prefixes;
  }

  /** {@code claims}: the claim set qits-deployments answered, sent whole. */
  private static void claimsFromBody(JsonNode recorded, GcConfig config, RunContext context) {
    ObjectNode answer = JSON.createObjectNode();
    answer.set("claims", recorded.path("claims").deepCopy());
    seed(context, GcProcess.CLAIMS_IDP_CLIENTS, answer);
  }

  private static void seedIfPresent(RunContext context, String stepId, JsonNode answer) {
    if (answer != null) {
      seed(context, stepId, answer);
    }
  }

  private static void seed(RunContext context, String stepId, JsonNode answer) {
    context.record(stepId, answer, answer.toString());
  }

  // --- running a row -----------------------------------------------------------------------------

  /**
   * The gc process as shipped: {@code META-INF/microprofile-config.properties}, outside CDI. The
   * one thing not as shipped is the wait between two asks of a 503 pin read (qits-1733): it is a
   * no-op, so a row that answers 503 never holds the suite for the shipped thirty seconds.
   */
  static GcProcess process(GcConfig config) {
    GcProcess process = new GcProcess();
    process.config = config;
    process.sleeper = duration -> {};
    return process;
  }

  /** The shipped gc settings, read from the file the jar ships them in. */
  static GcConfig shippedConfig() {
    Properties shipped = new Properties();
    try (InputStream in =
        GcContract.class.getResourceAsStream("/META-INF/microprofile-config.properties")) {
      shipped.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    String prefix = "qits.orchestrator.gc.";
    GcConfig config = new GcConfig();
    config.imageKeepPrefixes = list(shipped.getProperty(prefix + "image-keep-prefixes"));
    config.branchKeepPrefixes = list(shipped.getProperty(prefix + "branch-keep-prefixes"));
    config.imageMinAge = Duration.parse(shipped.getProperty(prefix + "image-min-age"));
    config.volumeMinAge = Duration.parse(shipped.getProperty(prefix + "volume-min-age"));
    config.buildCacheKeepBytes =
        Long.parseLong(shipped.getProperty(prefix + "build-cache-keep-bytes"));
    config.builderCacheKeepBytes =
        Long.parseLong(shipped.getProperty(prefix + "builder-cache-keep-bytes"));
    config.tagsSweepCallTimeout =
        Duration.parse(shipped.getProperty(prefix + "tags-sweep-call-timeout"));
    config.pinsRetryAttempts =
        Integer.parseInt(shipped.getProperty(prefix + "pins.retry.attempts"));
    config.pinsRetryDelay = Duration.parse(shipped.getProperty(prefix + "pins.retry.delay"));
    return config;
  }

  private static List<String> list(String value) {
    return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  static StepDefinition step(GcProcess process, String stepId) {
    return process.steps().stream()
        .filter(s -> s.id().equals(stepId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no gc step " + stepId));
  }

  /** {@code row}'s step, arranged to send the recorded request, calling {@code peers}. */
  static StepResult run(Row row, PeerClient peers) {
    Operation op = recorded(row);
    GcConfig config = shippedConfig();
    RunContext context = new RunContext(UUID.randomUUID(), false, peers);
    row.arrange().apply(op.body() == null ? null : examples(op.body(), op), config, context);
    return step(process(config), row.stepId()).body().run(context);
  }

  /** The step run against the provider's WHOLE recorded answer, through a fake. */
  static StepResult againstRecording(Row row) {
    FakePeers fake = ContractPeers.fake();
    fake.answer(
        recorded(row).examplePath(),
        FakePeers.Scripted.ok(
            masters(row).body(row.interaction().state(), row.interaction().operationId())));
    return run(row, fake);
  }

  /**
   * The step run against the provider's recorded answer cut to what the row {@code consumes}: the
   * same line as {@link #againstRecording}, or {@code consumes} misses a field the step reads.
   */
  static StepResult againstConsumed(Row row) {
    JsonNode whole =
        masters(row).json(row.interaction().state(), row.interaction().operationId());
    List<List<String>> paths = new ArrayList<>();
    for (String path : row.interaction().consumes()) {
      List<String> steps = new ArrayList<>();
      for (String key : path.split("\\.")) {
        if (key.endsWith("[]")) {
          steps.add(key.substring(0, key.length() - 2));
          steps.add("[]");
        } else {
          steps.add(key);
        }
      }
      paths.add(steps);
    }
    FakePeers fake = ContractPeers.fake();
    fake.answer(
        recorded(row).examplePath(), FakePeers.Scripted.ok(consumed(whole, paths).toString()));
    return run(row, fake);
  }

  /** {@code node} holding only {@code paths}; an array read without fields keeps empty elements. */
  private static JsonNode consumed(JsonNode node, List<List<String>> paths) {
    if (paths.stream().anyMatch(List::isEmpty)) {
      return node;
    }
    if (node.isArray()) {
      List<List<String>> rest = new ArrayList<>();
      for (List<String> path : paths) {
        if (path.getFirst().equals("[]")) {
          rest.add(path.subList(1, path.size()));
        }
      }
      ArrayNode out = JSON.createArrayNode();
      for (JsonNode element : node) {
        boolean bare = rest.stream().allMatch(List::isEmpty);
        out.add(bare && element.isContainerNode() ? JSON.createObjectNode() : consumed(element, rest));
      }
      return out;
    }
    ObjectNode out = JSON.createObjectNode();
    node.properties()
        .forEach(
            field -> {
              List<List<String>> rest = new ArrayList<>();
              for (List<String> path : paths) {
                if (path.getFirst().equals(field.getKey())) {
                  rest.add(path.subList(1, path.size()));
                }
              }
              if (!rest.isEmpty()) {
                out.set(field.getKey(), consumed(field.getValue(), rest));
              }
            });
    return out;
  }

  /** {@code value} with every {@code "{param}"} string replaced by the state's example. */
  private static JsonNode examples(JsonNode value, Operation op) {
    if (value.isTextual()) {
      return TextNode.valueOf(op.example(value.asText()));
    }
    if (value.isArray()) {
      ArrayNode out = JSON.createArrayNode();
      value.forEach(element -> out.add(examples(element, op)));
      return out;
    }
    if (value.isObject()) {
      ObjectNode out = JSON.createObjectNode();
      value.properties().forEach(field -> out.set(op.example(field.getKey()), examples(field.getValue(), op)));
      return out;
    }
    return value;
  }

  private GcContract() {}
}
