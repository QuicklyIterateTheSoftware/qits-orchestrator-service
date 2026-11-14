package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.gc.ArtifactsCleanup;
import eu.wohlben.qits.orchestrator.process.gc.GcExclusion;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The artifacts cleanup doors (ticket qits-2138): the pins read and handed on, qits-artifacts'
 * answer handed back as it came, the sweep and a gc run kept apart, and who may knock.
 *
 * <p>The peers are {@link FakePeers}, so the urls in the assertions are the shipped {@code %test}
 * ones and nothing reaches a network. A pin answer is never scripted as a 503 here: the shipped
 * retry would wait out thirty seconds five times.
 */
@QuarkusTest
@TestSecurity(user = "operator", roles = "qits:admin")
class ArtifactsCleanupApiTest {

  private static final String BASE = "/orchestrator/api/artifacts/repositories";

  private static final ObjectMapper JSON = new ObjectMapper();

  /** The five pin answers, keyed by the member qits-artifacts takes each one as. */
  private static final Map<String, String> PINS =
      Map.of(
          "deployments", "{\"pins\":[{\"applicationName\":\"qits-ci\",\"shas\":[\"abc\"]}]}",
          "ciDaemon", "{\"daemonName\":\"qits-ci-daemon\",\"daemonVersion\":\"1\"}",
          "dependencies", "{\"repositories\":[{\"name\":\"qits-ci-service\"}],\"pins\":[]}",
          "workspaceLaunches", "{\"pins\":[{\"image\":\"qits/workspace\",\"version\":\"0\"}]}",
          "projectLaunches", "{\"pins\":[{\"image\":\"qits/project-agent\",\"version\":\"0\"}]}");

  private static final Map<String, String> PIN_PATHS =
      Map.of(
          "deployments", "/deployments/api/pins",
          "ciDaemon", "/ci/api/daemon",
          "dependencies", "/maintenance/api/pins",
          "workspaceLaunches", "/workspaces/api/pins",
          "projectLaunches", "/projects/api/pins");

  /** qits-artifacts' answers, as text with a spelling no re-serialisation would keep. */
  private static final String LISTING =
      "{\"executable\":true,  \"repositories\":[{\"repository\":\"daemons\",\"reclaimableBytes\":7}]}";

  private static final String PLAN = "{\"repository\":\"daemons\",  \"executable\":true}";

  private static final String RECEIPT =
      "{\"repository\":\"daemons\",\"aborted\":null,"
          + "\"sweep\":{\"blobsUnlinked\":2,\"bytesReclaimed\":2048}}";

  @Inject FakePeers peers;

  @Inject RunStore runs;

  @Inject RunExecutor executor;

  @Inject ArtifactsCleanup cleanup;

  @Inject GcExclusion exclusion;

  @BeforeEach
  void scriptThePinsAndQitsArtifacts() {
    awaitNoActiveGcRun();
    peers.reset();
    PINS.forEach((member, body) -> peers.answer(PIN_PATHS.get(member), FakePeers.Scripted.ok(body)));
    peers.answer("/artifacts/api/gc/repositories", FakePeers.Scripted.ok(LISTING));
    peers.answer("/artifacts/api/gc/repositories/daemons/plan", FakePeers.Scripted.ok(PLAN));
    peers.answer("/artifacts/api/gc/repositories/daemons/sweep", FakePeers.Scripted.ok(RECEIPT));
  }

  // --- the pins, and what goes back -------------------------------------------------------------

  @Test
  void theListingReadsEveryPinAndAnswersQitsArtifactsTextVerbatim() throws Exception {
    String answered =
        given()
            .when()
            .get(BASE)
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();
    assertEquals(LISTING, answered, "the text qits-artifacts sent, not a re-serialisation");

    List<String> sent = peers.bodiesFor("/artifacts/api/gc/repositories");
    assertEquals(1, sent.size());
    JsonNode pins = JSON.readTree(sent.getFirst()).path("pins");
    assertEquals(PINS.keySet(), fieldNames(pins));
    for (Map.Entry<String, String> pin : PINS.entrySet()) {
      assertEquals(JSON.readTree(pin.getValue()), pins.get(pin.getKey()), pin.getKey());
    }
    assertTrue(
        peers.calls.stream().allMatch(call -> call.url().startsWith("http://dev-qits-")),
        "the shipped %test peers: " + peers.calls);
  }

  @Test
  void aPinSourceThatCannotBeReadIsAbsentFromTheBodyAndTheRequestIsStillMade() throws Exception {
    peers.answer("/ci/api/daemon", FakePeers.Scripted.status(500, "{\"message\":\"boom\"}"));
    peers.answer("/projects/api/pins", FakePeers.Scripted.unreachable("qits-projects: no route"));

    given().when().get(BASE + "/daemons/plan").then().statusCode(200).body("executable", equalTo(true));

    JsonNode pins =
        JSON.readTree(peers.bodiesFor("/artifacts/api/gc/repositories/daemons/plan").getFirst())
            .path("pins");
    assertEquals(
        java.util.Set.of("deployments", "dependencies", "workspaceLaunches"), fieldNames(pins));
    assertFalse(pins.has("ciDaemon"), "a failed read is absent, not null");
  }

  @Test
  void thePlanAnswersQitsArtifactsStatusAndBodyA404Included() {
    String plan = given().when().get(BASE + "/daemons/plan").then().statusCode(200).extract().asString();
    assertEquals(PLAN, plan);

    peers.answer(
        "/artifacts/api/gc/repositories/nothing-here/plan",
        FakePeers.Scripted.status(404, "{\"message\":\"no repository nothing-here\"}"));
    String missing =
        given()
            .when()
            .get(BASE + "/nothing-here/plan")
            .then()
            .statusCode(404)
            .contentType(ContentType.JSON)
            .extract()
            .asString();
    assertEquals("{\"message\":\"no repository nothing-here\"}", missing);
  }

  @Test
  void theRepositoryIsOnePathSegmentOnTheWayOut() {
    peers.answer(
        "/artifacts/api/gc/repositories/a%20b%2Fc/plan", FakePeers.Scripted.ok("{\"ok\":true}"));
    // Sent as written: RestAssured would otherwise encode the percent signs a second time.
    given()
        .urlEncodingEnabled(false)
        .when()
        .get(BASE + "/a%20b%2Fc/plan")
        .then()
        .statusCode(200)
        .body("ok", equalTo(true));
    assertEquals(1, peers.bodiesFor("/artifacts/api/gc/repositories/a%20b%2Fc/plan").size());
  }

  @Test
  void qitsArtifactsRefusingThisServicesCredentialIsA502NotTheCallersRefusal() {
    peers.answer(
        "/artifacts/api/gc/repositories/daemons/plan",
        FakePeers.Scripted.status(403, "{\"message\":\"forbidden\"}"));
    given()
        .when()
        .get(BASE + "/daemons/plan")
        .then()
        .statusCode(502)
        .body("message", containsString("refused qits-orchestrator's own credential with 403"));
  }

  @Test
  void qitsArtifactsThatCannotBeAskedIsA502() {
    peers.answer(
        "/artifacts/api/gc/repositories", FakePeers.Scripted.unreachable("qits-artifacts: no route"));
    given()
        .when()
        .get(BASE)
        .then()
        .statusCode(502)
        .body("message", containsString("qits-artifacts could not be asked"))
        .body("message", containsString("no route"));
  }

  @Test
  void theSweepAnswersTheReceiptAndHandsOnThePins() throws Exception {
    String receipt =
        given().when().post(BASE + "/daemons/sweep").then().statusCode(200).extract().asString();
    assertEquals(RECEIPT, receipt);
    JsonNode pins =
        JSON.readTree(peers.bodiesFor("/artifacts/api/gc/repositories/daemons/sweep").getFirst())
            .path("pins");
    assertEquals(PINS.keySet(), fieldNames(pins));
    assertFalse(exclusion.sweepInFlight(), "the sweep's registration ends with it");
  }

  // --- a sweep and a gc run never overlap -------------------------------------------------------

  @Test
  void aSweepIsRefusedWithA409WhileAGcRunIsActive() {
    peers.hold();
    UUID run;
    try {
      run = executor.start(GcProcess.KIND, RunTrigger.MANUAL, true);
      given()
          .when()
          .post(BASE + "/daemons/sweep")
          .then()
          .statusCode(409)
          .body("message", equalTo("a gc run is already active: " + run));
      assertTrue(peers.bodiesFor("/artifacts/api/gc/repositories/daemons/sweep").isEmpty());
      // The reads take no lock.
      peers.release();
      given().when().get(BASE + "/daemons/plan").then().statusCode(200);
    } finally {
      peers.release();
    }
    awaitNoActiveGcRun();
  }

  @Test
  void aGcRunIsRefusedWithA409WhileASweepIsInFlight() throws Exception {
    peers.hold();
    CompletableFuture<Integer> sweep = new CompletableFuture<>();
    Thread sweeper =
        new Thread(
            () -> {
              try {
                sweep.complete(
                    cleanup.sweep("daemons", "operator").answer().httpStatus());
              } catch (RuntimeException e) {
                sweep.completeExceptionally(e);
              }
            },
            "a-sweep-in-flight");
    sweeper.start();
    try {
      Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
      while (!exclusion.sweepInFlight() && Instant.now().isBefore(deadline)) {
        Thread.sleep(10);
      }
      assertTrue(exclusion.sweepInFlight(), "the sweep registered itself");
      given()
          .contentType(ContentType.JSON)
          .body("{\"dryRun\":true}")
          .when()
          .post("/orchestrator/api/processes/gc/runs")
          .then()
          .statusCode(409)
          .body("message", equalTo("a repository sweep is in flight: daemons"));
      runs.getEntityManager().clear();
      assertTrue(runs.active(GcProcess.KIND).isEmpty(), "no gc run was opened");
    } finally {
      peers.release();
    }
    assertEquals(200, sweep.get(30, TimeUnit.SECONDS));
    assertFalse(exclusion.sweepInFlight());
    // And once it answered, a gc run starts again.
    UUID run = executor.start(GcProcess.KIND, RunTrigger.MANUAL, true);
    assertTrue(run != null);
    awaitNoActiveGcRun();
  }

  // --- who may knock ----------------------------------------------------------------------------

  @Test
  @TestSecurity(user = "dyn-agent", roles = "qits:agent")
  void anAgentMayListPlanAndSweep() {
    given().when().get(BASE).then().statusCode(200);
    given().when().get(BASE + "/daemons/plan").then().statusCode(200);
    given().when().post(BASE + "/daemons/sweep").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "a-machine", roles = "qits:system")
  void aMachineMaySweep() {
    given().when().post(BASE + "/daemons/sweep").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "dyn-admin-workspace-agent", roles = "qits:admin-agent")
  void anAdminAgentMaySweep() {
    given().when().post(BASE + "/daemons/sweep").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "the-desk", roles = "qits:desk-admin")
  void aDeskAdminMaySweep() {
    given().when().post(BASE + "/daemons/sweep").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "someone", roles = "qits:reader")
  void aRoleOutsideTheSetIsRefusedOnEveryDoor() {
    given().when().get(BASE).then().statusCode(403);
    given().when().get(BASE + "/daemons/plan").then().statusCode(403);
    given().when().post(BASE + "/daemons/sweep").then().statusCode(403);
    assertTrue(peers.calls.isEmpty(), "a refused caller costs no pin read: " + peers.calls);
  }

  @Test
  @TestSecurity(authorizationEnabled = true)
  void anAnonymousCallerIsRefusedOnEveryDoor() {
    given().when().get(BASE).then().statusCode(401);
    given().when().get(BASE + "/daemons/plan").then().statusCode(401);
    given().when().post(BASE + "/daemons/sweep").then().statusCode(401);
    assertTrue(peers.calls.isEmpty(), "a refused caller costs no pin read: " + peers.calls);
  }

  // --- helpers ----------------------------------------------------------------------------------

  private static java.util.Set<String> fieldNames(JsonNode node) {
    java.util.Set<String> names = new java.util.TreeSet<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }

  /** The same wait as {@code ProcessApiTest.awaitNoActiveGcRun}, and the same clear. */
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
}
