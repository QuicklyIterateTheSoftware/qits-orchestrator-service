package eu.wohlben.qits.orchestrator.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The gc process against faked peers: the twenty-one steps, the bodies they send, the summaries they
 * read back, and what a broken pin read does.
 *
 * <p>The peers are {@link FakePeers}, an {@code @Alternative} over {@code PeerClient}'s two call
 * methods — so the urls asserted here are the real ones, resolved from the shipped target
 * configuration, and a wrong path or a wrong peer fails.
 */
@QuarkusTest
class GcProcessTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final String USAGE = "/containers/api/gc/usage";
  private static final String STORE = "/artifacts/api/store/summary";
  private static final String DEPLOYMENT_PINS = "/deployments/api/pins";
  private static final String CI_PIN = "/ci/api/daemon";
  private static final String DEPENDENCY_PINS = "/maintenance/api/pins";
  private static final String IMAGE_PINS = "/configuration/api/pins";
  private static final String WORKSPACE_LAUNCH_PINS = "/workspaces/api/pins";
  private static final String PROJECT_LAUNCH_PINS = "/projects/api/pins";
  private static final String PLAN = "/artifacts/api/gc/plan";
  private static final String SWEEP = "/artifacts/api/gc/sweep";
  private static final String IMAGES = "/containers/api/gc/images";
  private static final String VOLUMES = "/containers/api/gc/volumes";
  private static final String BUILD_CACHE = "/containers/api/gc/build-cache";
  private static final String CATALOGUE = "/projects/api/repositories";
  private static final String BRANCHES = "/workspaces/api/gc/branches";
  private static final String ENTRIES = "/configuration/api/gc/entries";
  private static final String TAGS = "/projects/api/gc/tags";
  private static final String CLAIMS = "/deployments/api/claims/idp-clients";
  private static final String SERVICE_CLIENTS = "/idp/api/gc/service-clients";

  @Inject RunExecutor executor;

  @Inject RunStore runs;

  @Inject FakePeers peers;

  @BeforeEach
  void scriptEveryPeerAsHealthy() {
    peers.reset();
    peers.answer(
        USAGE,
        FakePeers.Scripted.ok(
            """
            {"images":{"count":41,"active":19,"sizeBytes":43500000000,"reclaimableBytes":19300000000},
             "containers":{"count":19,"active":19,"sizeBytes":120000,"reclaimableBytes":0},
             "volumes":{"count":31,"active":22,"sizeBytes":8100000000,"reclaimableBytes":900000000},
             "buildCache":{"count":812,"active":0,"sizeBytes":35100000000,"reclaimableBytes":35100000000}}
            """));
    peers.answer(
        STORE,
        FakePeers.Scripted.ok(
            """
            {"diskTotalBytes":51200000000,"ociUnionBytes":50700000000,
             "docsBytes":164200000,"sbomBytes":112600000}
            """));
    peers.answer(
        DEPENDENCY_PINS,
        FakePeers.Scripted.ok(
            """
            {"generatedAt":"2026-09-04T20:00:00Z",
             "repositories":[{"name":"qits-githost-service","status":"OK"},
                             {"name":"qits-workspace-daemon","status":"OK"}],
             "pins":[{"ecosystem":"maven","name":"eu.wohlben.qits:qits-blobstore",
                      "version":"2026.903.85122","repository":"qits-githost-service",
                      "manifestPath":"pom.xml"},
                     {"ecosystem":"npm","name":"@qits/ui-components","version":"2026.902.204627",
                      "repository":"qits-githost-service","manifestPath":"package.json"},
                     {"ecosystem":"docker","name":"qits/workspace-base","version":"2026.902.143920",
                      "repository":"qits-workspace-daemon","manifestPath":"docker/Dockerfile"}]}
            """));
    peers.answer(
        IMAGE_PINS,
        FakePeers.Scripted.ok(
            """
            {"generatedAt":"2026-09-04T20:00:00Z",
             "pins":[{"image":"qits/project-agent","version":"2026.904.160152",
                      "application":"qits-projects","key":"env.QITS_PROJECTS_AGENT_IMAGE_VERSION"},
                     {"image":"qits/workspace","version":"2026.904.160522",
                      "application":"qits-workspaces","key":"env.QITS_WORKSPACE_IMAGE_VERSION"}]}
            """));
    // THE EFFECTIVE PINS, and they are a different tense from the configured ones above: each
    // launching service reads its OWN resolved config and says what a start would pull TODAY. The
    // versions here deliberately LAG qits-configuration's — that is the gap the two steps close.
    peers.answer(
        WORKSPACE_LAUNCH_PINS,
        FakePeers.Scripted.ok(
            """
            {"generatedAt":"2026-09-05T06:00:00Z",
             "pins":[{"image":"qits/workspace","version":"2026.903.120000","launches":"workspace"},
                     {"image":"qits/workspace-editor","version":"2026.904.100239",
                      "launches":"editor"}]}
            """));
    peers.answer(
        PROJECT_LAUNCH_PINS,
        FakePeers.Scripted.ok(
            """
            {"generatedAt":"2026-09-05T06:00:00Z",
             "pins":[{"image":"qits/project-agent","version":"2026.903.090000","launches":"agent"},
                     {"image":"qits/workspace","version":"2026.903.120000",
                      "launches":"refinement"}]}
            """));
    peers.answer(
        DEPLOYMENT_PINS,
        FakePeers.Scripted.ok(
            """
            {"pins":[{"applicationName":"qits-ci","shas":["aaa111","bbb222"]},
                     {"applicationName":"qits-gateway","shas":["ccc333"]}]}
            """));
    peers.answer(
        CI_PIN,
        FakePeers.Scripted.ok(
            """
            {"daemonName":"qits-ci-daemon","daemonVersion":"2026.815.120000",
             "previousDaemonVersion":"2026.814.101010","source":"pinned"}
            """));
    peers.answer(
        PLAN,
        FakePeers.Scripted.ok(
            """
            {"summary":{"executable":true,"headline":"…","identitiesCondemned":128,
                        "blobsSweepable":91,"reclaimableBytes":19300000000,
                        "reclaimable":"19.3 GB","withheldByGraceWindow":4,"types":[]}}
            """));
    peers.answer(
        SWEEP,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"aborted":null,
             "sweep":{"blobsUnlinked":91,"bytesReclaimed":17800000000,"withheldByGraceWindow":4,
                      "withheldBytes":100,"stillReferenced":7,"alreadyGone":0,"unlinkedBlobIds":[]}}
            """));
    peers.answer(
        IMAGES,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"examined":73,"bytesReclaimed":9400000000,
             "removed":[{"id":"sha256:1","tags":[],"sizeBytes":1,"reason":"unreferenced"}],
             "kept":[{"id":"sha256:2","tags":["qits/qits-ci:aaa111"],"sizeBytes":2,"reason":"pinned"},
                     {"id":"sha256:3","tags":[],"sizeBytes":3,"reason":"in-use"}],
             "failed":[]}
            """));
    peers.answer(
        VOLUMES,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,
             "removed":[{"name":"a","reason":"managed-no-row"},{"name":"b","reason":"anonymous"}],
             "kept":[{"name":"c","reason":"unmanaged"}],"failed":[]}
            """));
    peers.answer(
        BUILD_CACHE,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"host":{"reclaimedBytes":12600000000,"detail":"…"},
             "builders":[{"container":"buildx_buildkit_x0","reclaimedBytes":3100000000,
                          "detail":"…","error":null}]}
            """));
    peers.answer(
        CATALOGUE,
        FakePeers.Scripted.ok(
            """
            {"repositories":[{"id":"r-1","projectId":"p-1","name":"qits-ci","mainBranch":"main"},
                             {"id":"r-2","projectId":"p-1","name":"qits-docs","mainBranch":"main"}]}
            """));
    peers.answer(
        BRANCHES,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"repositoriesExamined":2,"branchesExamined":9,
             "removed":[{"repositoryId":"r-1","repositoryName":"qits-ci","branch":"old-work"}],
             "errors":[]}
            """));
    peers.answer(
        ENTRIES,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"examined":142,
             "removed":[{"application":"qits-docs","env":"dev","key":"env.OLD_FLAG",
                         "reason":"retired","lastDeclaredBy":"2026.815.120000"}],
             "kept":{"unpinned":12,"undeclaredPinnedVersion":3,"pinned":10,"inFlight":2,
                     "neverDeclared":85,"staged":4},
             "errors":[]}
            """));
    peers.answer(
        TAGS,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,"repositories":2,"examined":31,
             "deleted":[{"repository":"qits-ci","tag":"2026.814.090000","host":true,"twin":true}],
             "kept":{"newest":2,"pinnedVersion":2,"gitlink":1,"inFlight":0,"young":3},
             "errors":[]}
            """));
    peers.answer(
        CLAIMS,
        FakePeers.Scripted.ok(
            """
            {"claims":[{"clientId":"dev-qits-ci","applicationName":"qits-ci",
                        "environmentName":"dev","createdAt":"2026-09-01T10:00:00Z"},
                       {"clientId":"dev-qits-orchestrator","applicationName":"qits-orchestrator",
                        "environmentName":"dev","createdAt":"2026-09-02T10:00:00Z"}]}
            """));
    peers.answer(
        SERVICE_CLIENTS,
        FakePeers.Scripted.ok(
            """
            {"dryRun":false,
             "removed":[{"clientId":"dev-qits-retired","createdAt":"2026-08-01T10:00:00Z",
                         "createdBy":"qits-deployments","reason":"unclaimed"}],
             "kept":[{"clientId":"dev-qits-ci","createdAt":"2026-09-01T10:00:00Z",
                      "createdBy":"qits-deployments","reason":"claimed"},
                     {"clientId":"dev-qits-orchestrator","createdAt":"2026-09-02T10:00:00Z",
                      "createdBy":"qits-deployments","reason":"caller"}],
             "keptCounts":{"claimed":1,"grace":0,"caller":1}}
            """));
  }

  /**
   * A poll, the way a deployment does it.
   *
   * <p><b>The clear is load-bearing.</b> A {@code @QuarkusTest} holds ONE request context for the
   * whole test method, so one Hibernate session serves every read here and would answer every poll
   * out of its first-level cache — the run row would stay RUNNING forever while the worker thread
   * closed it in another session. In a deployment each poll is a fresh HTTP request with a session
   * of its own; clearing is how the suite gets the same thing.
   */
  private void nextPoll() {
    runs.getEntityManager().clear();
  }

  private OpRun awaitClosed(UUID id) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      nextPoll();
      OpRun run = runs.run(id).orElseThrow();
      if (!RunStatus.RUNNING.name().equals(run.status)) {
        return run;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("the gc run " + id + " never finished");
  }

  private Map<String, OpStep> stepsOf(UUID id) {
    nextPoll();
    Map<String, OpStep> byId = new LinkedHashMap<>();
    runs.steps(id).forEach(step -> byId.put(step.stepId, step));
    return byId;
  }

  /** An array node's values as strings — Jackson's own stream API is not stable across versions. */
  private static List<String> texts(JsonNode array) {
    List<String> values = new java.util.ArrayList<>();
    array.forEach(value -> values.add(value.asText()));
    return values;
  }

  private static JsonNode json(String text) {
    try {
      return JSON.readTree(text);
    } catch (Exception e) {
      throw new IllegalStateException("not JSON: " + text, e);
    }
  }

  @Test
  void aHealthyPlatformRunsAllTwentyOneStepsAndSummarisesEachFromTheAnswer() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.SUCCEEDED.name(), run.status);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(
        List.of(
            "usage.before",
            "artifacts.usage.before",
            "pins.deployments",
            "pins.ci",
            "pins.dependencies",
            "pins.images",
            "pins.workspaces",
            "pins.projects",
            "artifacts.plan",
            "artifacts.sweep",
            "containers.images",
            "containers.volumes",
            "containers.build-cache",
            "repos.catalogue",
            "branches.sweep",
            "configuration.entries",
            "tags.sweep",
            "claims.idp-clients",
            "idp.service-clients",
            "artifacts.usage.after",
            "usage.after"),
        runs.steps(id).stream().map(step -> step.stepId).toList());
    steps.values().forEach(step -> assertEquals(RunStatus.SUCCEEDED.name(), step.status, step.stepId));

    // THE SUMMARIES ARE READINGS OF THE ANSWER, never a second computation of it.
    assertEquals(
        "images 43.5 GB (19.3 GB reclaimable), build cache 35.1 GB",
        steps.get("usage.before").summary);
    // THE SECOND PLANE. The registry's bytes live in qits-artifacts' own store and the docker read
    // above cannot see one of them — a run measured by `docker system df` alone reported a platform
    // that was not growing while 50 GB of registry did.
    assertEquals(
        "store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)",
        steps.get("artifacts.usage.before").summary);
    assertEquals(
        "store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)",
        steps.get("artifacts.usage.after").summary);
    assertEquals("2 applications pinned", steps.get("pins.deployments").summary);
    assertEquals(
        "qits-ci-daemon 2026.815.120000 (previous 2026.814.101010)", steps.get("pins.ci").summary);
    assertEquals("3 manifest pins across 2 repositories", steps.get("pins.dependencies").summary);
    assertEquals("2 configured container images", steps.get("pins.images").summary);
    assertEquals(
        "2 launch images — what a workspace/editor start would pull today",
        steps.get("pins.workspaces").summary);
    assertEquals(
        "2 launch images — what an agent/refinement start would pull today",
        steps.get("pins.projects").summary);
    assertEquals("128 identities, 19.3 GB reclaimable, executable=true", steps.get("artifacts.plan").summary);
    assertEquals("91 blobs unlinked, 17.8 GB reclaimed", steps.get("artifacts.sweep").summary);
    assertEquals("1 images removed, 9.4 GB reclaimed, 2 kept", steps.get("containers.images").summary);
    assertEquals("2 volumes removed, 1 kept", steps.get("containers.volumes").summary);
    assertEquals(
        "host 12.6 GB reclaimed, 1 builder 3.1 GB reclaimed",
        steps.get("containers.build-cache").summary);
    assertEquals("2 repositories in the catalogue", steps.get("repos.catalogue").summary);
    assertEquals(
        "removed 1 of 9 branches across 2 repositories", steps.get("branches.sweep").summary);
    assertEquals(
        "removed 1 of 142 entries; kept: unpinned 12, undeclaredPinnedVersion 3, pinned 10,"
            + " inFlight 2, neverDeclared 85, staged 4; 0 errors",
        steps.get("configuration.entries").summary);
    assertEquals(
        "1 tags decommissioned across 2 repositories (1 host, 1 twin); kept: newest 2,"
            + " pinnedVersion 2, gitlink 1, inFlight 0, young 3; 0 errors",
        steps.get("tags.sweep").summary);
    assertEquals(
        "2 service-client claims across 2 applications", steps.get("claims.idp-clients").summary);
    assertEquals(
        "removed 1 service client (dev-qits-retired); kept 2 (claimed 1, grace 0, caller 1)",
        steps.get("idp.service-clients").summary);

    // The url is the shipped target plus the shipped path — a wrong peer would fail here. The host
    // is the derived dev-qits-<alias> form (no QITS_ENVIRONMENT in this suite, so the `dev`
    // fallback), never the bare alias.
    assertEquals(
        "http://dev-qits-deployments:8080/deployments/api/pins",
        steps.get("pins.deployments").requestUrl);
    assertEquals("GET", steps.get("pins.deployments").requestMethod);
    // Every pin read addressed at its own shipped target — a step pointed at the wrong one would
    // still answer here, because one FakePeers script serves all eight, and only the url says which
    // service was actually asked. That matters most for the two effective reads: they ride peers
    // this process already drives, so a path typo is the only thing that could send them astray.
    assertEquals(
        "http://dev-qits-platform-maintenance:8080/maintenance/api/pins",
        steps.get("pins.dependencies").requestUrl);
    assertEquals(
        "http://dev-qits-configuration:8080/configuration/api/pins",
        steps.get("pins.images").requestUrl);
    assertEquals(
        "http://dev-qits-workspaces:8080/workspaces/api/pins", steps.get("pins.workspaces").requestUrl);
    assertEquals(
        "http://dev-qits-projects:8080/projects/api/pins", steps.get("pins.projects").requestUrl);
    assertEquals(
        "http://dev-qits-artifacts:8080/artifacts/api/store/summary",
        steps.get("artifacts.usage.before").requestUrl);
    assertEquals(200, steps.get("usage.after").httpStatus);
    // The answer is stored whole, which is what an investigation reads.
    assertTrue(steps.get("containers.images").responseBody.contains("bytesReclaimed"));
    assertEquals(
        "http://dev-qits-configuration:8080/configuration/api/gc/entries",
        steps.get("configuration.entries").requestUrl);
    assertEquals("POST", steps.get("configuration.entries").requestMethod);
    assertEquals(
        "http://dev-qits-projects:8080/projects/api/gc/tags", steps.get("tags.sweep").requestUrl);
    assertEquals("POST", steps.get("tags.sweep").requestMethod);
    // The claims are read from the deployer, the sweep is asked of qits-idp — the ninth target.
    assertEquals(
        "http://dev-qits-deployments:8080/deployments/api/claims/idp-clients",
        steps.get("claims.idp-clients").requestUrl);
    assertEquals("GET", steps.get("claims.idp-clients").requestMethod);
    assertEquals(
        "http://dev-qits-idp:8080/idp/api/gc/service-clients",
        steps.get("idp.service-clients").requestUrl);
    assertEquals("POST", steps.get("idp.service-clients").requestMethod);
  }

  @Test
  void theServiceClientSweepDependsOnTheClaimsReadAloneAndEmbedsTheClaimsVerbatim() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    assertEquals(
        "claims.idp-clients",
        runs.steps(id).stream()
            .filter(step -> "idp.service-clients".equals(step.stepId))
            .findFirst()
            .orElseThrow()
            .dependsOn);

    JsonNode body = json(peers.bodiesFor(SERVICE_CLIENTS).getFirst());
    assertFalse(body.get("dryRun").asBoolean());
    // The deployer's claims array, unchanged — every field of every row, re-embedded rather than
    // projected onto a list of ids.
    assertEquals(
        json(
            """
            [{"clientId":"dev-qits-ci","applicationName":"qits-ci",
              "environmentName":"dev","createdAt":"2026-09-01T10:00:00Z"},
             {"clientId":"dev-qits-orchestrator","applicationName":"qits-orchestrator",
              "environmentName":"dev","createdAt":"2026-09-02T10:00:00Z"}]
            """),
        body.get("claims"));
    assertEquals(2, body.size(), "the body is dryRun and claims, nothing else: " + body);
  }

  @Test
  void aBrokenClaimsReadSkipsTheServiceClientSweepAndAsksQitsIdpForNothing() {
    peers.answer(CLAIMS, FakePeers.Scripted.unreachable("qits-deployments: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("claims.idp-clients").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("idp.service-clients").status);
    assertEquals("skipped: claims.idp-clients failed", steps.get("idp.service-clients").error);
    assertNull(steps.get("idp.service-clients").requestUrl);
    assertTrue(
        peers.bodiesFor(SERVICE_CLIENTS).isEmpty(),
        "no service client may be swept against claims nobody could read");
    // Nothing else waits on the claims: every other deleter and both closing measurements ran.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("tags.sweep").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("configuration.entries").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("usage.after").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("artifacts.usage.after").status);
  }

  @Test
  void aClaimsReadThatAnsweredNoClaimsIsRefusedRatherThanSentAsAnEmptyList() {
    // The read SUCCEEDED, so the edge lets the sweep run — and the body is what refuses: to qits-idp
    // an empty claim set means nothing is claimed, which would condemn every service client.
    peers.answer(CLAIMS, FakePeers.Scripted.ok("{\"claims\":[]}"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("claims.idp-clients").status);
    assertEquals(
        "0 service-client claims across 0 applications", steps.get("claims.idp-clients").summary);
    assertEquals(RunStatus.FAILED.name(), steps.get("idp.service-clients").status);
    assertTrue(
        steps.get("idp.service-clients").error.contains("answered no claims"),
        steps.get("idp.service-clients").error);
    assertNull(steps.get("idp.service-clients").requestUrl);
    assertTrue(peers.bodiesFor(SERVICE_CLIENTS).isEmpty(), "an empty claim set must never be sent");
  }

  @Test
  void aClaimsAnswerWithNoClaimsArrayIsRefusedTheSameWay() {
    peers.answer(CLAIMS, FakePeers.Scripted.ok("{\"message\":\"not the claims document\"}"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("idp.service-clients").status);
    assertTrue(peers.bodiesFor(SERVICE_CLIENTS).isEmpty(), "a missing claim set must never be sent");
  }

  @Test
  void theTagsSweepStepDependsOnAllSixPinsAndTheCatalogueAndEmbedsThePinsVerbatim() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    assertEquals(
        "pins.deployments,pins.ci,pins.dependencies,pins.images,pins.workspaces,pins.projects,"
            + "repos.catalogue",
        runs.steps(id).stream()
            .filter(step -> "tags.sweep".equals(step.stepId))
            .findFirst()
            .orElseThrow()
            .dependsOn);

    JsonNode body = json(peers.bodiesFor(TAGS).getFirst());
    assertFalse(body.get("dryRun").asBoolean());
    JsonNode pins = body.get("pins");
    // The same pins object artifacts.plan sends, verbatim — all six sources, not just the one
    // configuration.entries carries.
    assertEquals(
        "qits-ci", pins.get("deployments").get("pins").get(0).get("applicationName").asText());
    assertEquals("2026.815.120000", pins.get("ciDaemon").get("daemonVersion").asText());
    assertEquals(
        "eu.wohlben.qits:qits-blobstore",
        pins.get("dependencies").get("pins").get(0).get("name").asText());
    assertEquals(
        "qits/project-agent", pins.get("configuredImages").get("pins").get(0).get("image").asText());
    assertEquals(
        "2026.903.120000",
        pins.get("workspaceLaunches").get("pins").get(0).get("version").asText());
    assertEquals(
        "refinement", pins.get("projectLaunches").get("pins").get(1).get("launches").asText());
  }

  @Test
  void aBrokenDeploymentPinReadSkipsTheTagsSweepToo() {
    peers.answer(
        DEPLOYMENT_PINS, FakePeers.Scripted.unreachable("qits-platform-deployments: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("tags.sweep").status);
    assertEquals("skipped: pins.deployments failed", steps.get("tags.sweep").error);
    assertTrue(peers.bodiesFor(TAGS).isEmpty(), "no tag may be decommissioned without every pin source");
  }

  @Test
  void aBrokenCatalogueReadSkipsTheTagsSweepToo() {
    peers.answer(CATALOGUE, FakePeers.Scripted.unreachable("qits-projects: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("tags.sweep").status);
    assertEquals("skipped: repos.catalogue failed", steps.get("tags.sweep").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("branches.sweep").status);
  }

  @Test
  void theConfigurationEntriesStepDependsOnDeploymentPinsAloneAndEmbedsThemVerbatim() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    assertEquals(
        "pins.deployments",
        runs.steps(id).stream()
            .filter(step -> "configuration.entries".equals(step.stepId))
            .findFirst()
            .orElseThrow()
            .dependsOn);

    JsonNode body = json(peers.bodiesFor(ENTRIES).getFirst());
    assertFalse(body.get("dryRun").asBoolean());
    // Verbatim, the same discipline as the pins body: the deployments answer re-embedded whole
    // rather than re-shaped into a local keep-set.
    assertEquals(
        "qits-ci", body.get("deployments").get("pins").get(0).get("applicationName").asText());
    assertEquals(
        List.of("aaa111", "bbb222"),
        texts(body.get("deployments").get("pins").get(0).get("shas")));
  }

  @Test
  void aBrokenDeploymentPinReadSkipsTheConfigurationEntrySweepToo() {
    peers.answer(
        DEPLOYMENT_PINS, FakePeers.Scripted.unreachable("qits-platform-deployments: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("configuration.entries").status);
    assertEquals(
        "skipped: pins.deployments failed", steps.get("configuration.entries").error);
    assertTrue(peers.bodiesFor(ENTRIES).isEmpty(), "no entries may be swept without the pin");
  }

  @Test
  void thePinsGoToArtifactsVerbatimUnderTheSixNamesItReads() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    JsonNode planBody = json(peers.bodiesFor(PLAN).getFirst());
    JsonNode pins = planBody.get("pins");
    assertEquals(
        "qits-ci", pins.get("deployments").get("pins").get(0).get("applicationName").asText());
    assertEquals("2026.815.120000", pins.get("ciDaemon").get("daemonVersion").asText());
    // The two consumption sources, and they are the ones no deployment and no release date can
    // stand in for: what a repository's main still references, and what a launch would pull.
    assertEquals(
        "eu.wohlben.qits:qits-blobstore",
        pins.get("dependencies").get("pins").get(0).get("name").asText());
    assertEquals(
        "qits-workspace-daemon",
        pins.get("dependencies").get("repositories").get(1).get("name").asText());
    assertEquals(
        "qits/project-agent", pins.get("configuredImages").get("pins").get(0).get("image").asText());
    // …and the two EFFECTIVE ones, which no other member can stand in for: the workspace version
    // here is 2026.903.120000 while `configuredImages` already says 2026.904.160522, because
    // qits-workspaces has not been redeployed since. Both must survive the collection.
    assertEquals(
        "2026.903.120000",
        pins.get("workspaceLaunches").get("pins").get(0).get("version").asText());
    assertEquals(
        "refinement", pins.get("projectLaunches").get("pins").get(1).get("launches").asText());
    // The sweep is handed the same document, not a re-read of the peers.
    assertEquals(planBody, json(peers.bodiesFor(SWEEP).getFirst()));
  }

  @Test
  void aPinSourceThatAnsweredSomethingUnreadableIsAbsentFromTheBodyRatherThanNull() {
    // 200 with a body that is not JSON: the step SUCCEEDS — a peer that answered is not a failure —
    // but there is no tree to embed, so the member is absent rather than null. It is the belt behind
    // the edges (a 1 MiB truncation does the same thing), and qits-artifacts reads an absent member
    // as that source being unanswered, which aborts the sweep on its side.
    peers.answer(IMAGE_PINS, FakePeers.Scripted.status(200, "{\"pins\":[…truncated"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("pins.images").status);

    JsonNode pins = json(peers.bodiesFor(PLAN).getFirst()).get("pins");
    assertTrue(pins.has("deployments"));
    assertTrue(pins.has("ciDaemon"));
    assertTrue(pins.has("dependencies"));
    assertTrue(pins.has("workspaceLaunches"));
    assertTrue(pins.has("projectLaunches"));
    assertFalse(pins.has("configuredImages"), "an unreadable source must not be sent as anything");
  }

  @Test
  void anEffectiveLaunchPinNobodyCouldReadStopsTheRegistryAndLeavesTheHostSweepAlone() {
    // The same fail-closed shape as the dependency read, and it is asserted separately because the
    // two effective sources ride peers that ALREADY had steps here: qits-workspaces is the branch
    // sweep's peer. A broken launch-pin read must skip the registry and leave the branch sweep — a
    // step with no pin of its own — entirely alone.
    peers.answer(
        WORKSPACE_LAUNCH_PINS, FakePeers.Scripted.unreachable("qits-workspaces: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("pins.workspaces").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.plan").status);
    assertEquals("skipped: pins.workspaces failed", steps.get("artifacts.plan").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.sweep").status);
    assertTrue(peers.bodiesFor(PLAN).isEmpty(), "no plan may be asked for without every pin source");
    // tags.sweep carries all six pin edges too, so the same broken source skips it.
    assertEquals(RunStatus.SKIPPED.name(), steps.get("tags.sweep").status);
    assertEquals("skipped: pins.workspaces failed", steps.get("tags.sweep").error);
    assertTrue(peers.bodiesFor(TAGS).isEmpty(), "no tag may be decommissioned without every pin source");

    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("pins.projects").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.images").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("branches.sweep").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.build-cache").status);
  }

  @Test
  void theImageKeepSetIsEveryDeploymentPinAsALocalTagPlusTheConfiguredPrefixes() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    awaitClosed(id);

    JsonNode body = json(peers.bodiesFor(IMAGES).getFirst());
    assertFalse(body.get("dryRun").asBoolean());
    assertEquals("PT6H", body.get("minAge").asText());
    assertEquals(
        List.of("qits/qits-ci:aaa111", "qits/qits-ci:bbb222", "qits/qits-gateway:ccc333"),
        texts(body.get("keep")));
    assertEquals(
        List.of("qits/build-images/", "qits/graalvmce-musl-builder"),
        texts(body.get("keepPrefixes")));

    JsonNode volumes = json(peers.bodiesFor(VOLUMES).getFirst());
    assertEquals("PT24H", volumes.get("minAge").asText());
    JsonNode cache = json(peers.bodiesFor(BUILD_CACHE).getFirst());
    // TWO BUDGETS, and the second is the point: a buildx_buildkit_* container is a bootstrap-time
    // cache, so it is pruned to near-nothing while the host's warm cache is kept. One number for
    // both left a 13.7 GB bootstrap builder untouched every night.
    assertEquals(10737418240L, cache.get("keepStorageBytes").asLong());
    assertEquals(1073741824L, cache.get("builderKeepStorageBytes").asLong());
  }

  @Test
  void aDryRunCallsEveryPeerWithItsOwnFlagAndSkipsTheSweepThatDeletes() {
    UUID id = executor.start("gc", RunTrigger.MANUAL, true);
    OpRun run = awaitClosed(id);

    // A skipped step does not fail a run.
    assertEquals(RunStatus.SUCCEEDED.name(), run.status);
    assertTrue(run.dryRun);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.sweep").status);
    assertEquals("dry run", steps.get("artifacts.sweep").error);
    assertNull(steps.get("artifacts.sweep").requestUrl);
    assertTrue(peers.bodiesFor(SWEEP).isEmpty(), "a dry run must not call the sweep");

    // Everything else still runs, and every peer that can plan is told to plan.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("artifacts.plan").status);
    assertTrue(json(peers.bodiesFor(IMAGES).getFirst()).get("dryRun").asBoolean());
    assertTrue(json(peers.bodiesFor(VOLUMES).getFirst()).get("dryRun").asBoolean());
    assertTrue(json(peers.bodiesFor(BUILD_CACHE).getFirst()).get("dryRun").asBoolean());
    // THE BRANCH SWEEP IS NOT WITHHELD: qits-workspaces has a real dry mode, so the flag travels
    // in the body and the projection carries exactly the fields the sweep judges by.
    JsonNode branches = json(peers.bodiesFor(BRANCHES).getFirst());
    assertTrue(branches.get("dryRun").asBoolean());
    assertEquals("r-1", branches.get("repositories").get(0).get("id").asText());
    assertEquals("main", branches.get("repositories").get(0).get("mainBranch").asText());
    assertEquals(List.of("environment/"), texts(branches.get("keepPrefixes")));
    // AND THE MEASUREMENT STILL HAPPENS. usage.after depends on the sweep, but a POLICY skip is
    // not a failure: the run did exactly what it was asked, so nothing after it has a reason not to
    // run. The live dry run that reported "skipped: artifacts.sweep failed" here — with all nine
    // calls answering 200 — is what this line exists for.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("usage.after").status);
    assertEquals(200, steps.get("usage.after").httpStatus);
    assertNull(steps.get("usage.after").error);
    // …and so does the registry's closing measurement, which hangs off the withheld step alone.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("artifacts.usage.after").status);
    // CONFIGURATION.ENTRIES IS NOT WITHHELD EITHER: qits-configuration has a real dry mode, like
    // qits-workspaces' branch sweep, so it is asked for real — never policy-skipped like the
    // registry's own sweep.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("configuration.entries").status);
    assertTrue(json(peers.bodiesFor(ENTRIES).getFirst()).get("dryRun").asBoolean());
    // TAGS.SWEEP IS NOT WITHHELD EITHER, the same reasoning: qits-projects judges identically on a
    // dry run and deletes nothing.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("tags.sweep").status);
    assertTrue(json(peers.bodiesFor(TAGS).getFirst()).get("dryRun").asBoolean());
    // NOR IS THE SERVICE-CLIENT SWEEP: qits-idp honours dryRun itself, so a dry night still reports
    // which clients it would have removed.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("idp.service-clients").status);
    assertTrue(json(peers.bodiesFor(SERVICE_CLIENTS).getFirst()).get("dryRun").asBoolean());
  }

  @Test
  void anUnreadablePinSetStopsEverythingThatDeletesOnItAndNothingElse() {
    peers.answer(
        DEPLOYMENT_PINS, FakePeers.Scripted.unreachable("qits-platform-deployments: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("pins.deployments").status);
    assertTrue(steps.get("pins.deployments").error.contains("no route"));

    // FAIL-CLOSED: nothing deletes against a keep-set that could not be read.
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.plan").status);
    assertEquals("skipped: pins.deployments failed", steps.get("artifacts.plan").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.sweep").status);
    assertEquals("skipped: pins.deployments failed", steps.get("artifacts.sweep").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("containers.images").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("tags.sweep").status);
    assertEquals("skipped: pins.deployments failed", steps.get("tags.sweep").error);
    assertTrue(peers.bodiesFor(PLAN).isEmpty(), "no plan may be asked for without pins");
    assertTrue(peers.bodiesFor(IMAGES).isEmpty(), "no image may be swept without pins");
    assertTrue(peers.bodiesFor(TAGS).isEmpty(), "no tag may be decommissioned without pins");

    // AND EVERYTHING THAT NEEDS NO PINS STILL RUNS. That is the point of the edges being per step:
    // both the volume sweep and the build-cache prune hang off usage.before alone, so a broken pin
    // read costs the platform no reclaim it could have had. The build cache is the larger half of
    // the measured problem, and it has no keep-set to be wrong about.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("usage.before").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.volumes").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.build-cache").status);
    assertFalse(peers.bodiesFor(BUILD_CACHE).isEmpty(), "the prune needs no pins and must be asked for");

    // usage.after DOES cascade here, and the difference from the dry-run case is the whole point:
    // two of the four steps it depends on were skipped BY A FAILURE, so an "after" figure would be
    // measuring a run that could not happen rather than one that chose not to.
    assertEquals(RunStatus.SKIPPED.name(), steps.get("usage.after").status);
    // The registry's own opening measurement needs no pin either, so the run still records what the
    // store held — which is the figure the whole second plane exists for.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("artifacts.usage.before").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.usage.after").status);
  }

  @Test
  void aDependencyPinNobodyCouldReadStopsTheRegistryAndLeavesTheHostSweepAlone() {
    // The new pin sources are the registry's, not the host's: qits-platform-maintenance says what
    // repositories' mains still reference, which is a keep-set for maven, npm and oci identities in
    // qits-artifacts. A host image is kept by a DEPLOYMENT pin, which answered — so the image sweep
    // must still run, exactly as the build-cache prune does.
    peers.answer(
        DEPENDENCY_PINS, FakePeers.Scripted.unreachable("qits-platform-maintenance: no route"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("pins.dependencies").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.plan").status);
    assertEquals("skipped: pins.dependencies failed", steps.get("artifacts.plan").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.sweep").status);
    assertEquals("skipped: pins.dependencies failed", steps.get("artifacts.sweep").error);
    assertTrue(peers.bodiesFor(PLAN).isEmpty(), "no plan may be asked for without every pin source");
    assertEquals(RunStatus.SKIPPED.name(), steps.get("tags.sweep").status);
    assertEquals("skipped: pins.dependencies failed", steps.get("tags.sweep").error);

    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("pins.images").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.images").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("containers.build-cache").status);
  }

  @Test
  void aPeerThatRefusesTheCallIsAFailedStepNamingTheStatus() {
    peers.answer(PLAN, FakePeers.Scripted.status(401, "{\"message\":\"unauthorized\"}"));

    UUID id = executor.start("gc", RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("artifacts.plan").status);
    assertEquals(401, steps.get("artifacts.plan").httpStatus);
    assertTrue(steps.get("artifacts.plan").error.endsWith("answered 401"), steps.get("artifacts.plan").error);
    // The refusal's body is kept, because it usually says which audience was expected.
    assertTrue(steps.get("artifacts.plan").responseBody.contains("unauthorized"));
    assertEquals(RunStatus.SKIPPED.name(), steps.get("artifacts.sweep").status);
    assertEquals("skipped: artifacts.plan failed", steps.get("artifacts.sweep").error);
  }
}
