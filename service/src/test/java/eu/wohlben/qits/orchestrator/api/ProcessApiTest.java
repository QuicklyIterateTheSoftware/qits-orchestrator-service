package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The REST boundary: the shapes, the status codes and the refusals.
 *
 * <p>The addresses are the shipped ones — the suite inherits {@code
 * quarkus.rest.path=/orchestrator/api} from main's application.properties rather than re-declaring
 * it — so a change to the segment fails here rather than in a deployment.
 *
 * <p><b>Every test runs as an operator</b> ({@code @TestSecurity}, {@code qits:admin}), so the
 * shipped {@code @RolesAllowed} list is exercised rather than bypassed. Forward-auth and its {@code
 * %test} dev user go with qits-1338 (epic qits-543). That a real request must carry a bearer the
 * shipped tenant validates is pinned in {@code PackagedSurfaceIT}, where the identity contract is
 * real.
 *
 * <p>The peers are faked, so a run started here reaches no network.
 */
@QuarkusTest
@TestSecurity(user = "operator", roles = "qits:admin")
class ProcessApiTest {

  private static final String BASE = "/orchestrator/api";

  @Inject FakePeers peers;

  @Inject RunStore runs;

  @BeforeEach
  void scriptTheHappyPath() {
    // A neighbouring class (AdminAgentAccessTest, AgentReadAccessTest) may have started a gc run
    // against the one shared app and DB and returned without waiting; drain it before this class
    // scripts the peers and holds them, or `start` below gets a 409 that is not this class's own.
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
    peers.answer("/artifacts/api/gc/plan", FakePeers.Scripted.ok("{\"summary\":{\"executable\":true}}"));
    peers.answer("/artifacts/api/gc/sweep", FakePeers.Scripted.ok("{\"sweep\":{\"blobsUnlinked\":0}}"));
    peers.answer("/containers/api/gc/images", FakePeers.Scripted.ok("{\"removed\":[],\"kept\":[]}"));
    peers.answer("/containers/api/gc/volumes", FakePeers.Scripted.ok("{\"removed\":[],\"kept\":[]}"));
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
  }

  /**
   * Waits for any run a neighbouring {@code @QuarkusTest} class left RUNNING — of any kind, since
   * one worker runs them all — so this class's
   * own single-flight assertions are not tripped by somebody else's leak. See {@code
   * GcDeployTriggerTest.awaitNoActiveRun} for the same wait and the clear it needs.
   */
  private void awaitNoActiveRun() {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      runs.getEntityManager().clear();
      if (java.util.stream.Stream.of(GcProcess.KIND, ScanProcess.KIND, SbomCheckProcess.KIND)
          .allMatch(kind -> runs.active(kind).isEmpty())) {
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

  /** Starts a run and returns its id. */
  private String start(boolean dryRun) {
    return given()
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":" + dryRun + "}")
        .when()
        .post(BASE + "/processes/gc/runs")
        .then()
        .statusCode(202)
        .extract()
        .path("id");
  }

  /** Polls the run the way the client does, until it is no longer RUNNING. */
  private String awaitClosed(String id) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      String status =
          given().when().get(BASE + "/runs/" + id).then().statusCode(200).extract().path("status");
      if (!"RUNNING".equals(status)) {
        return status;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("run " + id + " never finished");
  }

  @Test
  void theProcessListingIsTheDefinitionTheClientDrawsTheGraphFrom() {
    given()
        .when()
        .get(BASE + "/processes")
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("kind", hasItem("gc"))
        .body("find { it.kind == 'gc' }.name", equalTo("Garbage collection"))
        .body("find { it.kind == 'gc' }.description", notNullValue())
        .body("find { it.kind == 'gc' }.steps.size()", equalTo(22))
        .body("find { it.kind == 'gc' }.steps[0].id", equalTo("usage.before"))
        .body("find { it.kind == 'gc' }.steps[0].target", equalTo("containers"))
        .body("find { it.kind == 'gc' }.steps[0].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[1].id", equalTo("artifacts.usage.before"))
        .body("find { it.kind == 'gc' }.steps[1].target", equalTo("artifacts"))
        .body("find { it.kind == 'gc' }.steps[4].id", equalTo("pins.dependencies"))
        .body("find { it.kind == 'gc' }.steps[4].target", equalTo("maintenance"))
        // The two launch reads, on peers this process already drives — no new target, and the
        // target is asserted precisely because it is shared with a step that does something else.
        .body("find { it.kind == 'gc' }.steps[5].id", equalTo("pins.workspaces"))
        .body("find { it.kind == 'gc' }.steps[5].target", equalTo("workspaces"))
        .body("find { it.kind == 'gc' }.steps[6].id", equalTo("pins.projects"))
        .body("find { it.kind == 'gc' }.steps[6].target", equalTo("projects"))
        .body("find { it.kind == 'gc' }.steps[8].id", equalTo("artifacts.plan"))
        .body(
            "find { it.kind == 'gc' }.steps[8].dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.workspaces",
                "pins.projects"))
        .body("find { it.kind == 'gc' }.steps[13].id", equalTo("repos.catalogue"))
        .body("find { it.kind == 'gc' }.steps[13].target", equalTo("projects"))
        .body("find { it.kind == 'gc' }.steps[14].id", equalTo("branches.sweep"))
        .body("find { it.kind == 'gc' }.steps[14].target", equalTo("workspaces"))
        .body(
            "find { it.kind == 'gc' }.steps[14].dependsOn",
            contains(
                "repos.catalogue",
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.workspaces",
                "pins.projects"))
        .body("find { it.kind == 'gc' }.steps[15].id", equalTo("tags.sweep"))
        .body("find { it.kind == 'gc' }.steps[15].target", equalTo("projects"))
        .body(
            "find { it.kind == 'gc' }.steps[15].dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.workspaces",
                "pins.projects",
                "repos.catalogue"))
        // The service-client sweep (qits-878): claims read from the deployer, the eighth peer asked to
        // delete what nothing claims — the claims read first, then every pin read (qits-1175).
        .body("find { it.kind == 'gc' }.steps[16].id", equalTo("claims.idp-clients"))
        .body("find { it.kind == 'gc' }.steps[16].target", equalTo("deployments"))
        .body("find { it.kind == 'gc' }.steps[16].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[17].id", equalTo("idp.service-clients"))
        .body("find { it.kind == 'gc' }.steps[17].target", equalTo("idp"))
        .body(
            "find { it.kind == 'gc' }.steps[17].dependsOn",
            contains(
                "claims.idp-clients",
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.workspaces",
                "pins.projects"))
        // qits-mirror's pair (qits-2253): the image pins read from qits-maintenance with no edge,
        // and the sweep with none either — it runs, without pins, when the read failed.
        .body("find { it.kind == 'gc' }.steps[7].id", equalTo("pins.mirror-images"))
        .body("find { it.kind == 'gc' }.steps[7].target", equalTo("maintenance"))
        .body("find { it.kind == 'gc' }.steps[7].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[18].id", equalTo("mirror.sweep"))
        .body("find { it.kind == 'gc' }.steps[18].target", equalTo("mirror"))
        .body("find { it.kind == 'gc' }.steps[18].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[19].id", equalTo("artifacts.usage.after"))
        .body("find { it.kind == 'gc' }.steps[19].dependsOn", contains("artifacts.sweep"));
  }

  /**
   * The scan and SBOM check processes (ticket qits-2490) beside gc: their steps and edges, and
   * which of the three may be a dry run — what the client reads to hide the button.
   */
  @Test
  void theListingCarriesTheScanAndSbomCheckProcessesAndWhichKindMayRunDry() {
    given()
        .when()
        .get(BASE + "/processes")
        .then()
        .statusCode(200)
        .body("kind", contains("gc", "sbom-check", "scan"))
        .body("find { it.kind == 'gc' }.dryRunnable", equalTo(true))
        .body("find { it.kind == 'scan' }.name", equalTo("Manifest scan"))
        .body("find { it.kind == 'scan' }.dryRunnable", equalTo(false))
        .body(
            "find { it.kind == 'scan' }.steps.id",
            contains("internal.request", "internal.follow", "external.request", "external.follow"))
        .body("find { it.kind == 'scan' }.steps.target", everyItem(equalTo("maintenance")))
        .body("find { it.kind == 'scan' }.steps[1].dependsOn", contains("internal.request"))
        .body("find { it.kind == 'scan' }.steps[2].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'scan' }.steps[3].dependsOn", contains("external.request"))
        .body("find { it.kind == 'sbom-check' }.name", equalTo("SBOM check"))
        .body("find { it.kind == 'sbom-check' }.dryRunnable", equalTo(false))
        .body("find { it.kind == 'sbom-check' }.steps.id", contains("check"))
        .body("find { it.kind == 'sbom-check' }.steps[0].target", equalTo("maintenance"));
  }

  /**
   * A dry run of a process that has none is a 400 with the platform's envelope, and nothing is
   * started — the listing has no run of it afterwards.
   */
  @Test
  void aDryRunOfAProcessWithoutOneIsRefusedWithA400AndStartsNothing() {
    for (String kind : java.util.List.of(ScanProcess.KIND, SbomCheckProcess.KIND)) {
      given()
          .contentType(ContentType.JSON)
          .body("{\"dryRun\":true}")
          .when()
          .post(BASE + "/processes/" + kind + "/runs")
          .then()
          .statusCode(400)
          .contentType(ContentType.JSON)
          .body("message", containsString("the " + kind + " process has no dry run"));
      given()
          .when()
          .get(BASE + "/processes/" + kind + "/runs")
          .then()
          .statusCode(200)
          .body("findAll { it.dryRun == true }.size()", equalTo(0));
    }
  }

  /** A scan run: both scans requested and followed, each request in its own scope. */
  @Test
  void aScanRunRequestsAndFollowsBothScans() {
    String scanId = "00000000-0000-4000-8000-0000000000a1";
    peers.answer(
        ScanProcess.SCANS_PATH, FakePeers.Scripted.status(202, "{\"id\":\"" + scanId + "\"}"));
    peers.answer(
        ScanProcess.SCANS_PATH + "/" + scanId,
        FakePeers.Scripted.ok(
            "{\"id\":\"" + scanId + "\",\"scope\":\"INTERNAL\",\"status\":\"SUCCEEDED\","
                + "\"message\":null}"));
    String id = startKind(ScanProcess.KIND);
    assertEquals("SUCCEEDED", awaitClosed(id));
    given()
        .when()
        .get(BASE + "/runs/" + id)
        .then()
        .statusCode(200)
        .body("kind", equalTo("scan"))
        .body("dryRun", equalTo(false))
        .body("steps.status", everyItem(equalTo("SUCCEEDED")))
        .body("steps[0].request.method", equalTo("POST"))
        .body("steps[0].request.body", equalTo("{\"scope\":\"INTERNAL\"}"))
        .body("steps[0].httpStatus", equalTo(202))
        .body("steps[1].request.method", equalTo("GET"))
        .body("steps[1].summary", equalTo("INTERNAL scan SUCCEEDED"))
        .body("steps[2].request.body", equalTo("{\"scope\":\"EXTERNAL\"}"));
  }

  /** An SBOM check run: one body-less POST, its report summarised. */
  @Test
  void anSbomCheckRunPostsTheCheckAndSummarisesTheReport() {
    peers.answer(
        SbomCheckProcess.RUNS_PATH,
        FakePeers.Scripted.status(
            202,
            "{\"ranAt\":\"2026-11-24T02:15:00Z\",\"filed\":true,\"entries\":[],"
                + "\"warnings\":[],\"tickets\":[]}"));
    String id = startKind(SbomCheckProcess.KIND);
    assertEquals("SUCCEEDED", awaitClosed(id));
    given()
        .when()
        .get(BASE + "/runs/" + id)
        .then()
        .statusCode(200)
        .body("kind", equalTo("sbom-check"))
        .body("steps[0].id", equalTo("check"))
        .body("steps[0].request.method", equalTo("POST"))
        .body("steps[0].request.body", nullValue())
        .body(
            "steps[0].summary",
            equalTo("0 versions without a usable SBOM, 0 tickets open, 0 warnings"));
  }

  /** Starts a real run of {@code kind} and returns its id. */
  private String startKind(String kind) {
    return given()
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":false}")
        .when()
        .post(BASE + "/processes/" + kind + "/runs")
        .then()
        .statusCode(202)
        .extract()
        .path("id");
  }

  @Test
  void aRunIsAcceptedWithItsIdAndTheListingShowsItAfterwards() {
    String id = start(false);
    assertEquals("SUCCEEDED", awaitClosed(id));

    given()
        .when()
        .get(BASE + "/processes/gc/runs?limit=5")
        .then()
        .statusCode(200)
        .body("id", hasItem(id))
        .body("find { it.id == '" + id + "' }.kind", equalTo("gc"))
        .body("find { it.id == '" + id + "' }.trigger", equalTo("manual"))
        .body("find { it.id == '" + id + "' }.dryRun", equalTo(false))
        .body("find { it.id == '" + id + "' }.status", equalTo("SUCCEEDED"))
        .body("find { it.id == '" + id + "' }.startedAt", notNullValue())
        .body("find { it.id == '" + id + "' }.finishedAt", notNullValue())
        .body("find { it.id == '" + id + "' }.summary", containsString("usage.before SUCCEEDED"));
  }

  @Test
  void theRunDetailCarriesEveryStepWithItsRequestAndTheAnswerItGot() {
    String id = start(true);
    assertEquals("SUCCEEDED", awaitClosed(id));

    given()
        .when()
        .get(BASE + "/runs/" + id)
        .then()
        .statusCode(200)
        .body("id", equalTo(id))
        .body("kind", equalTo("gc"))
        .body("dryRun", equalTo(true))
        .body("steps.size()", equalTo(22))
        .body("steps[0].id", equalTo("usage.before"))
        .body("steps[0].name", equalTo("Disk usage before"))
        .body("steps[0].target", equalTo("containers"))
        .body("steps[0].status", equalTo("SUCCEEDED"))
        .body("steps[0].httpStatus", equalTo(200))
        .body("steps[0].request.method", equalTo("GET"))
        .body(
            "steps[0].request.url",
            equalTo("http://dev-qits-containers:8080/containers/api/gc/usage"))
        .body("steps[0].request.body", nullValue())
        // `response` is the peer's JSON as TEXT, stored as it arrived. A client parses it.
        .body("steps[0].response", containsString("\"sizeBytes\""))
        .body("steps[0].error", nullValue())
        .body("steps[0].summary", notNullValue())
        // The registry sweep is the step a dry run does not make…
        .body("steps[9].id", equalTo("artifacts.sweep"))
        .body("steps[9].status", equalTo("SKIPPED"))
        .body("steps[9].error", equalTo("dry run"))
        .body("steps[9].request", nullValue())
        .body(
            "steps[9].dependsOn",
            contains(
                "artifacts.plan",
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.workspaces",
                "pins.projects"))
        // …while the branch sweep runs on a dry run too — qits-workspaces judges identically and
        // deletes nothing, so the request carries the flag rather than being withheld.
        .body("steps[14].id", equalTo("branches.sweep"))
        .body("steps[14].status", equalTo("SUCCEEDED"))
        .body("steps[14].request.method", equalTo("POST"))
        .body("steps[14].request.body", containsString("\"dryRun\":true"))
        .body("steps[14].request.body", containsString("\"mainBranch\":\"main\""))
        .body("steps[14].request.body", containsString("environment/"))
        .body("steps[14].request.body", containsString("release/"))
        .body("steps[14].summary", containsString("removed 1 of 4 branches"))
        // tags.sweep is not withheld either — qits-projects judges identically on a dry run — and
        // it carries the same {"pins": {...}} object the plan does, not just one source.
        .body("steps[15].id", equalTo("tags.sweep"))
        .body("steps[15].status", equalTo("SUCCEEDED"))
        .body("steps[15].request.method", equalTo("POST"))
        .body("steps[15].request.body", containsString("\"dryRun\":true"))
        .body("steps[15].request.body", containsString("\"applicationName\":\"qits-ci\""))
        .body("steps[15].summary", containsString("tags decommissioned"))
        // idp.service-clients is not withheld either — qits-idp honours dryRun itself — and it
        // carries the deployer's claims array as it arrived.
        .body("steps[16].id", equalTo("claims.idp-clients"))
        .body("steps[16].status", equalTo("SUCCEEDED"))
        .body("steps[17].id", equalTo("idp.service-clients"))
        .body("steps[17].status", equalTo("SUCCEEDED"))
        .body("steps[17].request.method", equalTo("POST"))
        .body(
            "steps[17].request.url",
            equalTo("http://dev-qits-idp:8080/idp/api/gc/service-clients"))
        .body("steps[17].request.body", containsString("\"dryRun\":true"))
        .body("steps[17].request.body", containsString("\"clientId\":\"dev-qits-ci\""))
        // …and neither is the mirror's sweep (qits-2253): qits-mirror honours dryRun itself, and the
        // image pins ride in the body as qits-maintenance answered them.
        .body("steps[18].id", equalTo("mirror.sweep"))
        .body("steps[18].status", equalTo("SUCCEEDED"))
        .body(
            "steps[18].request.url", equalTo("http://dev-qits-mirror:8080/mirror/api/gc/sweep"))
        .body("steps[18].request.body", containsString("\"dryRun\":true"))
        .body("steps[18].request.body", containsString("\"version\":\"24-alpine\""))
        // The registry's own measurement, taken twice like the host's — the plane a `docker system
        // df` receipt cannot see.
        .body("steps[19].id", equalTo("artifacts.usage.after"))
        .body("steps[19].status", equalTo("SUCCEEDED"))
        .body(
            "steps[19].request.url",
            equalTo("http://dev-qits-artifacts:8080/artifacts/api/store/summary"))
        // qits-toggles' retirement run, last (qits-2703): a dry gc run asks for a dry one.
        .body("steps[21].id", equalTo("toggles.retirement"))
        .body("steps[21].target", equalTo("toggles"))
        .body("steps[21].status", equalTo("SUCCEEDED"))
        .body(
            "steps[21].request.url",
            equalTo("http://dev-qits-toggles:8080/toggles/api/retirement/runs?dryRun=true"));
  }

  @Test
  void aBodylessReadKeepsTheRequestLineAndTheSizesAndDropsTheBodies() {
    String id = start(true);
    assertEquals("SUCCEEDED", awaitClosed(id));

    io.restassured.path.json.JsonPath full =
        given().when().get(BASE + "/runs/" + id).then().statusCode(200).extract().jsonPath();
    String response = full.getString("steps[14].response");
    // The size is the stored text's, in UTF-8 bytes, and the full read carries it too.
    assertEquals(
        response.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
        full.getInt("steps[14].responseBytes"));
    // `bodies=true` is the default spelled out: the same answer.
    assertEquals(
        full.getList("steps"),
        given()
            .when()
            .get(BASE + "/runs/" + id + "?bodies=true")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getList("steps"));

    given()
        .when()
        .get(BASE + "/runs/" + id + "?bodies=false")
        .then()
        .statusCode(200)
        .body("id", equalTo(id))
        .body("status", equalTo("SUCCEEDED"))
        .body("steps.size()", equalTo(22))
        .body("steps.response", everyItem(nullValue()))
        .body("steps.findAll { it.request != null }.request.body", everyItem(nullValue()))
        // branches.sweep POSTs a body: the request line stays, its body and the answer go.
        .body("steps[14].id", equalTo("branches.sweep"))
        .body("steps[14].request.method", equalTo("POST"))
        .body(
            "steps[14].request.url",
            equalTo("http://dev-qits-workspaces:8080/workspaces/api/gc/branches"))
        .body("steps[14].request.body", nullValue())
        .body("steps[14].response", nullValue())
        .body("steps[14].responseBytes", equalTo(full.getInt("steps[14].responseBytes")))
        .body("steps[14].httpStatus", equalTo(200))
        .body("steps[14].summary", containsString("removed 1 of 4 branches"))
        // The step a dry run does not make stored nothing, so it has no size either.
        .body("steps[9].id", equalTo("artifacts.sweep"))
        .body("steps[9].request", nullValue())
        .body("steps[9].responseBytes", nullValue());
  }

  @Test
  void theStepDoorServesOneStepWholeAsTheRunReadDoes() {
    String id = start(true);
    assertEquals("SUCCEEDED", awaitClosed(id));

    java.util.Map<String, Object> fromRun =
        given()
            .when()
            .get(BASE + "/runs/" + id)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getMap("steps[14]");
    java.util.Map<String, Object> step =
        given()
            .when()
            .get(BASE + "/runs/" + id + "/steps/branches.sweep")
            .then()
            .statusCode(200)
            .body("id", equalTo("branches.sweep"))
            .body("request.body", containsString("\"dryRun\":true"))
            .body("response", notNullValue())
            .body("responseBytes", notNullValue())
            .extract()
            .jsonPath()
            .getMap("$");
    assertEquals(fromRun, step);
  }

  @Test
  void theStepDoorIs404ForAnUnknownRunAndForAStepTheRunDoesNotHold() {
    String id = start(true);
    assertEquals("SUCCEEDED", awaitClosed(id));

    given()
        .when()
        .get(BASE + "/runs/" + UUID.randomUUID() + "/steps/artifacts.plan")
        .then()
        .statusCode(404)
        .body("message", containsString("no run"));
    given().when().get(BASE + "/runs/not-a-uuid/steps/artifacts.plan").then().statusCode(404);
    given()
        .when()
        .get(BASE + "/runs/" + id + "/steps/no.such-step")
        .then()
        .statusCode(404)
        .body("message", containsString("has no step 'no.such-step'"));
  }

  @Test
  void aSecondRunIsRefusedWithA409WhileOneIsActive() {
    CountDownLatch gate = peers.hold();
    try {
      String first = start(false);
      given()
          .contentType(ContentType.JSON)
          .body("{\"dryRun\":false}")
          .when()
          .post(BASE + "/processes/gc/runs")
          .then()
          .statusCode(409)
          .body("message", containsString("already active"));
      gate.countDown();
      assertEquals("SUCCEEDED", awaitClosed(first));
    } finally {
      peers.release();
    }
  }

  @Test
  void anUnknownKindIs404OnEveryRouteThatTakesOne() {
    given().when().get(BASE + "/processes/no-such/runs").then().statusCode(404);
    given()
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":true}")
        .when()
        .post(BASE + "/processes/no-such/runs")
        .then()
        .statusCode(404)
        .body("message", containsString("no technical process"));
  }

  @Test
  void anUnknownRunIs404AndSoIsAnIdThatIsNotAUuid() {
    given()
        .when()
        .get(BASE + "/runs/" + java.util.UUID.randomUUID())
        .then()
        .statusCode(404)
        .body("message", containsString("no run"));
    given().when().get(BASE + "/runs/not-a-uuid").then().statusCode(404);
  }

  @Test
  void aRunRecordedBeforeTheConfigurationEntrySweepWasRemovedStillReadsBack() {
    // Epic qits-765 removed the configuration.entries step and the `configuration` peer. A run row
    // written before that carries both, and it must still read back whole: a stored step row holds
    // its own id, name, target and edges and is never looked up in the current definition.
    UUID runId = UUID.randomUUID();
    Instant at = Instant.parse("2026-10-01T03:00:00Z");
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              OpRun run = new OpRun();
              run.id = runId;
              run.kind = GcProcess.KIND;
              run.trigger = "SCHEDULED";
              run.dryRun = false;
              run.status = "SUCCEEDED";
              run.startedAt = at;
              run.finishedAt = at.plusSeconds(60);
              run.summary = "configuration.entries ok";
              run.persist();
              OpStep step = new OpStep();
              step.id = UUID.randomUUID();
              step.runId = runId;
              step.seq = 14;
              step.stepId = "configuration.entries";
              step.name = "Retired configuration entries";
              step.target = "configuration";
              step.dependsOn =
                  "pins.deployments,pins.ci,pins.dependencies,pins.workspaces,pins.projects";
              step.status = "SUCCEEDED";
              step.startedAt = at;
              step.finishedAt = at.plusSeconds(1);
              step.httpStatus = 200;
              step.requestMethod = "POST";
              step.requestUrl = "http://dev-qits-configuration:8080/configuration/api/gc/entries";
              step.summary = "removed 0 of 0 entries";
              step.persist();
            });

    given()
        .when()
        .get(BASE + "/runs/" + runId)
        .then()
        .statusCode(200)
        .body("steps.size()", equalTo(1))
        .body("steps[0].id", equalTo("configuration.entries"))
        .body("steps[0].target", equalTo("configuration"))
        .body("steps[0].dependsOn.size()", equalTo(5))
        .body("steps[0].status", equalTo("SUCCEEDED"))
        .body("steps[0].summary", equalTo("removed 0 of 0 entries"));
  }
}
