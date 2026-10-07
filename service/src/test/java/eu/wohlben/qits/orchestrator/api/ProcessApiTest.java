package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
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
 * <p><b>No test sends an identity header</b>, and that is not a hole: qits-auth-core ships a {@code
 * %test} dev user carrying {@code qits:admin} and {@code qits:system}, so the shipped {@code
 * @RolesAllowed} pair is exercised rather than bypassed. That a real request must carry the pair is
 * pinned in {@code PackagedSurfaceIT}, where the identity contract is real.
 *
 * <p>The peers are faked, so a run started here reaches no network.
 */
@QuarkusTest
class ProcessApiTest {

  private static final String BASE = "/orchestrator/api";

  @Inject FakePeers peers;

  @Inject RunStore runs;

  @BeforeEach
  void scriptTheHappyPath() {
    // A neighbouring class (AdminAgentAccessTest, AgentReadAccessTest) may have started a gc run
    // against the one shared app and DB and returned without waiting; drain it before this class
    // scripts the peers and holds them, or `start` below gets a 409 that is not this class's own.
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
        "/configuration/api/pins",
        FakePeers.Scripted.ok(
            "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\"1\","
                + "\"application\":\"qits-workspaces\"}]}"));
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
        "/configuration/api/gc/entries",
        FakePeers.Scripted.ok(
            "{\"dryRun\":false,\"examined\":1,\"removed\":[],"
                + "\"kept\":{\"unpinned\":0,\"undeclaredPinnedVersion\":0,\"pinned\":1,"
                + "\"inFlight\":0,\"neverDeclared\":0,\"staged\":0},\"errors\":[]}"));
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
  }

  /**
   * Waits for any gc run a neighbouring {@code @QuarkusTest} class left RUNNING, so this class's
   * own single-flight assertions are not tripped by somebody else's leak. See {@code
   * GcDeployTriggerTest.awaitNoActiveRun} for the same wait and the clear it needs.
   */
  private void awaitNoActiveGcRun() {
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
        .body("find { it.kind == 'gc' }.steps.size()", equalTo(21))
        .body("find { it.kind == 'gc' }.steps[0].id", equalTo("usage.before"))
        .body("find { it.kind == 'gc' }.steps[0].target", equalTo("containers"))
        .body("find { it.kind == 'gc' }.steps[0].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[1].id", equalTo("artifacts.usage.before"))
        .body("find { it.kind == 'gc' }.steps[1].target", equalTo("artifacts"))
        .body("find { it.kind == 'gc' }.steps[4].id", equalTo("pins.dependencies"))
        .body("find { it.kind == 'gc' }.steps[4].target", equalTo("maintenance"))
        .body("find { it.kind == 'gc' }.steps[5].id", equalTo("pins.images"))
        .body("find { it.kind == 'gc' }.steps[5].target", equalTo("configuration"))
        // The two effective reads, on peers this process already drives — no new target, and the
        // target is asserted precisely because it is shared with a step that does something else.
        .body("find { it.kind == 'gc' }.steps[6].id", equalTo("pins.workspaces"))
        .body("find { it.kind == 'gc' }.steps[6].target", equalTo("workspaces"))
        .body("find { it.kind == 'gc' }.steps[7].id", equalTo("pins.projects"))
        .body("find { it.kind == 'gc' }.steps[7].target", equalTo("projects"))
        .body("find { it.kind == 'gc' }.steps[8].id", equalTo("artifacts.plan"))
        .body(
            "find { it.kind == 'gc' }.steps[8].dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.images",
                "pins.workspaces",
                "pins.projects"))
        .body("find { it.kind == 'gc' }.steps[13].id", equalTo("repos.catalogue"))
        .body("find { it.kind == 'gc' }.steps[13].target", equalTo("projects"))
        .body("find { it.kind == 'gc' }.steps[14].id", equalTo("branches.sweep"))
        .body("find { it.kind == 'gc' }.steps[14].target", equalTo("workspaces"))
        .body("find { it.kind == 'gc' }.steps[14].dependsOn", contains("repos.catalogue"))
        .body("find { it.kind == 'gc' }.steps[15].id", equalTo("configuration.entries"))
        .body("find { it.kind == 'gc' }.steps[15].target", equalTo("configuration"))
        .body("find { it.kind == 'gc' }.steps[15].dependsOn", contains("pins.deployments"))
        .body("find { it.kind == 'gc' }.steps[16].id", equalTo("tags.sweep"))
        .body("find { it.kind == 'gc' }.steps[16].target", equalTo("projects"))
        .body(
            "find { it.kind == 'gc' }.steps[16].dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.images",
                "pins.workspaces",
                "pins.projects",
                "repos.catalogue"))
        // The service-client sweep (qits-878): claims read from the deployer, the ninth peer asked to
        // delete what nothing claims — and the read is its only edge.
        .body("find { it.kind == 'gc' }.steps[17].id", equalTo("claims.idp-clients"))
        .body("find { it.kind == 'gc' }.steps[17].target", equalTo("deployments"))
        .body("find { it.kind == 'gc' }.steps[17].dependsOn", equalTo(java.util.List.of()))
        .body("find { it.kind == 'gc' }.steps[18].id", equalTo("idp.service-clients"))
        .body("find { it.kind == 'gc' }.steps[18].target", equalTo("idp"))
        .body("find { it.kind == 'gc' }.steps[18].dependsOn", contains("claims.idp-clients"))
        .body("find { it.kind == 'gc' }.steps[19].id", equalTo("artifacts.usage.after"))
        .body("find { it.kind == 'gc' }.steps[19].dependsOn", contains("artifacts.sweep"));
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
        .body("steps.size()", equalTo(21))
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
                "pins.images",
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
        .body("steps[14].summary", containsString("removed 1 of 4 branches"))
        // configuration.entries is not withheld either — qits-configuration has a real dry mode,
        // the same reasoning as the branch sweep.
        .body("steps[15].id", equalTo("configuration.entries"))
        .body("steps[15].status", equalTo("SUCCEEDED"))
        .body("steps[15].request.method", equalTo("POST"))
        .body("steps[15].request.body", containsString("\"dryRun\":true"))
        .body("steps[15].request.body", containsString("\"applicationName\":\"qits-ci\""))
        // tags.sweep is not withheld either — qits-projects judges identically on a dry run — and
        // it carries the same {"pins": {...}} object the plan does, not just one source.
        .body("steps[16].id", equalTo("tags.sweep"))
        .body("steps[16].status", equalTo("SUCCEEDED"))
        .body("steps[16].request.method", equalTo("POST"))
        .body("steps[16].request.body", containsString("\"dryRun\":true"))
        .body("steps[16].request.body", containsString("\"applicationName\":\"qits-ci\""))
        .body("steps[16].summary", containsString("tags decommissioned"))
        // idp.service-clients is not withheld either — qits-idp honours dryRun itself — and it
        // carries the deployer's claims array as it arrived.
        .body("steps[17].id", equalTo("claims.idp-clients"))
        .body("steps[17].status", equalTo("SUCCEEDED"))
        .body("steps[18].id", equalTo("idp.service-clients"))
        .body("steps[18].status", equalTo("SUCCEEDED"))
        .body("steps[18].request.method", equalTo("POST"))
        .body(
            "steps[18].request.url",
            equalTo("http://dev-qits-idp:8080/idp/api/gc/service-clients"))
        .body("steps[18].request.body", containsString("\"dryRun\":true"))
        .body("steps[18].request.body", containsString("\"clientId\":\"dev-qits-ci\""))
        // The registry's own measurement, taken twice like the host's — the plane a `docker system
        // df` receipt cannot see.
        .body("steps[19].id", equalTo("artifacts.usage.after"))
        .body("steps[19].status", equalTo("SUCCEEDED"))
        .body(
            "steps[19].request.url",
            equalTo("http://dev-qits-artifacts:8080/artifacts/api/store/summary"));
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
}
