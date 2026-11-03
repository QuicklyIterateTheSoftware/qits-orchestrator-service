package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
 * {@code qits:agent}, a commissioned agent's own role: it reads every GET route, and may also start
 * a {@code gc} run — {@code gc} is the only technical process this service knows, and an owner has
 * decided an agent may trigger it, dry-run or real, with no further guard.
 *
 * <p>Each test names its identity with {@code @TestSecurity}, so the identity holds exactly the
 * role named (forward-auth and its {@code %test} dev user go with qits-1338, epic qits-543).
 */
@QuarkusTest
class AgentReadAccessTest {

  private static final String BASE = "/orchestrator/api";

  @Inject RunStore runs;

  private static final String CALLER = "dyn-workspace-agent";

  @Test
  @TestSecurity(user = CALLER, roles = "qits:agent")
  void anAgentReadsTheProcessesAndTheirRuns() {
    given().get(BASE + "/processes").then().statusCode(200);
    given().get(BASE + "/processes/gc/runs").then().statusCode(200);
  }

  @Test
  @TestSecurity(user = CALLER, roles = "qits:agent")
  void anAgentReadsARun() {
    given()
        .get(BASE + "/runs/" + UUID.randomUUID())
        .then()
        .statusCode(not(anyOf(is(401), is(403))));
  }

  @Test
  @TestSecurity(user = CALLER, roles = "qits:agent")
  void anAgentStartsAGcRun() {
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
  @TestSecurity(user = CALLER, roles = "qits:reader")
  void aRoleOutsideTheBoundaryIsStillRefused() {
    given().get(BASE + "/processes").then().statusCode(403);
  }
}
