package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
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
 * <p>Each request names its identity in {@code X-Qits-User} / {@code X-Qits-Roles}, so the {@code
 * %test} dev user does not apply and the identity holds exactly the role sent.
 */
@QuarkusTest
class AgentReadAccessTest {

  private static final String BASE = "/orchestrator/api";

  @Inject RunStore runs;

  private static RequestSpecification as(String role) {
    return given().header("X-Qits-User", "dyn-workspace-agent").header("X-Qits-Roles", role);
  }

  private static RequestSpecification agent() {
    return as("qits:agent");
  }

  @Test
  void anAgentReadsTheProcessesAndTheirRuns() {
    agent().get(BASE + "/processes").then().statusCode(200);
    agent().get(BASE + "/processes/gc/runs").then().statusCode(200);
  }

  @Test
  void anAgentReadsARun() {
    agent()
        .get(BASE + "/runs/" + UUID.randomUUID())
        .then()
        .statusCode(not(anyOf(is(401), is(403))));
  }

  @Test
  void anAgentStartsAGcRun() {
    String id =
        agent()
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
  void aRoleOutsideTheBoundaryIsStillRefused() {
    as("qits:reader").get(BASE + "/processes").then().statusCode(403);
  }
}
