package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;

import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * {@code qits:admin-agent} — issued alongside {@code qits:agent} to the credential of an ADMIN
 * workspace's own coding-agent container (qits-628). The rule for now is blunt: it is admitted
 * everywhere {@code qits:admin} is, spelled explicitly beside it on every {@code @RolesAllowed}
 * list rather than through any augmentor or prefix match, so a later per-endpoint narrowing can
 * remove it one door at a time.
 *
 * <p>This pins the write door — starting a run — against an identity carrying ONLY {@code
 * qits:admin-agent}, no {@code qits:admin} and no {@code qits:agent}, the same way {@link
 * AgentReadAccessTest} pins {@code qits:agent}. Each test names its identity with {@code
 * @TestSecurity}, so the identity holds exactly the role named (forward-auth and its {@code %test}
 * dev user go with qits-1338, epic qits-543).
 */
@QuarkusTest
class AdminAgentAccessTest {

  private static final String BASE = "/orchestrator/api";

  @Inject RunStore runs;

  private static final String CALLER = "dyn-admin-workspace-agent";

  @Test
  @TestSecurity(user = CALLER, roles = "qits:admin-agent")
  void anAdminAgentStartsAGcRun() {
    String id =
        given()
            .contentType(ContentType.JSON)
            .body("{\"dryRun\":true}")
            .post(BASE + "/processes/gc/runs")
            .then()
            .statusCode(202)
            .extract()
            .path("id");
    // The run executes against FakePeers, possibly unscripted here, so it fails fast — but a
    // neighbouring @QuarkusTest class shares this app and DB, and RunStore.open refuses a second
    // gc run while one is RUNNING, so wait this one out rather than leaking it onto that class.
    awaitClosed(UUID.fromString(id));
  }

  /** Waits until the given run is no longer RUNNING. See {@code GcDeployTriggerTest.awaitClosed}. */
  private void awaitClosed(UUID id) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      runs.getEntityManager().clear();
      OpRun run = runs.run(id).orElseThrow();
      if (!RunStatus.RUNNING.name().equals(run.status)) {
        return;
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
  @TestSecurity(user = CALLER, roles = "qits:admin-agent")
  void anAdminAgentReadsTheProcesses() {
    given().get(BASE + "/processes").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = CALLER, roles = "qits:reader")
  void aRoleOutsideTheBoundaryIsStillRefusedOnTheWriteDoor() {
    given()
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":true}")
        .post(BASE + "/processes/gc/runs")
        .then()
        .statusCode(403);
  }

  /** {@code qits:desk-admin} (epic qits-1621): admitted wherever {@code qits:admin-agent} is. */
  @Test
  @TestSecurity(user = CALLER, roles = "qits:desk-admin")
  void aDeskAdminStartsAGcRun() {
    String id =
        given()
            .contentType(ContentType.JSON)
            .body("{\"dryRun\":true}")
            .post(BASE + "/processes/gc/runs")
            .then()
            .statusCode(202)
            .extract()
            .path("id");
    awaitClosed(UUID.fromString(id));
  }

  @Test
  @TestSecurity(user = CALLER, roles = "qits:desk-admin")
  void aDeskAdminReadsTheProcesses() {
    given().get(BASE + "/processes").then().statusCode(200);
  }
}
