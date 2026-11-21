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
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The artifacts cleanup listing (ticket qits-2138): the pins read and handed on, qits-artifacts'
 * answer handed back as it came, and who may knock. The plan and the sweep are gone (qits-2477).
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


  @Inject FakePeers peers;

  @Inject RunStore runs;


  @BeforeEach
  void scriptThePinsAndQitsArtifacts() {
    awaitNoActiveGcRun();
    peers.reset();
    PINS.forEach((member, body) -> peers.answer(PIN_PATHS.get(member), FakePeers.Scripted.ok(body)));
    peers.answer("/artifacts/api/gc/repositories", FakePeers.Scripted.ok(LISTING));
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

    given().when().get(BASE).then().statusCode(200).body("executable", equalTo(true));

    JsonNode pins =
        JSON.readTree(peers.bodiesFor("/artifacts/api/gc/repositories").getFirst()).path("pins");
    assertEquals(
        java.util.Set.of("deployments", "dependencies", "workspaceLaunches"), fieldNames(pins));
    assertFalse(pins.has("ciDaemon"), "a failed read is absent, not null");
  }

  @Test
  void thePlanAndTheSweepAreGone() {
    given().when().get(BASE + "/daemons/plan").then().statusCode(404);
    given().when().post(BASE + "/daemons/sweep").then().statusCode(404);
    assertTrue(peers.calls.isEmpty(), "no door, no pin read: " + peers.calls);
  }

  @Test
  void qitsArtifactsRefusingThisServicesCredentialIsA502NotTheCallersRefusal() {
    peers.answer(
        "/artifacts/api/gc/repositories",
        FakePeers.Scripted.status(403, "{\"message\":\"forbidden\"}"));
    given()
        .when()
        .get(BASE)
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

  // --- who may knock ----------------------------------------------------------------------------

  @Test
  @TestSecurity(user = "dyn-agent", roles = "qits:agent")
  void anAgentMayList() {
    given().when().get(BASE).then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "a-machine", roles = "qits:system")
  void aMachineMayList() {
    given().when().get(BASE).then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "dyn-admin-workspace-agent", roles = "qits:admin-agent")
  void anAdminAgentMayList() {
    given().when().get(BASE).then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "the-desk", roles = "qits:desk-admin")
  void aDeskAdminMayList() {
    given().when().get(BASE).then().statusCode(200);
  }

  @Test
  @TestSecurity(user = "someone", roles = "qits:reader")
  void aRoleOutsideTheSetIsRefused() {
    given().when().get(BASE).then().statusCode(403);
    assertTrue(peers.calls.isEmpty(), "a refused caller costs no pin read: " + peers.calls);
  }

  @Test
  @TestSecurity(authorizationEnabled = true)
  void anAnonymousCallerIsRefused() {
    given().when().get(BASE).then().statusCode(401);
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
