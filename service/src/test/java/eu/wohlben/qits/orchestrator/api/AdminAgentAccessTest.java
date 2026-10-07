package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
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
 * AgentReadAccessTest} pins {@code qits:agent}. Each request names its identity in {@code
 * X-Qits-User} / {@code X-Qits-Roles}, so the {@code %test} dev user does not apply and the
 * identity holds exactly the role sent.
 */
@QuarkusTest
class AdminAgentAccessTest {

  private static final String BASE = "/orchestrator/api";

  private static RequestSpecification as(String role) {
    return given().header("X-Qits-User", "dyn-admin-workspace-agent").header("X-Qits-Roles", role);
  }

  private static RequestSpecification adminAgent() {
    return as("qits:admin-agent");
  }

  @Test
  void anAdminAgentStartsAGcRun() {
    adminAgent()
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":true}")
        .post(BASE + "/processes/gc/runs")
        .then()
        .statusCode(202);
  }

  @Test
  void anAdminAgentReadsTheProcesses() {
    adminAgent().get(BASE + "/processes").then().statusCode(200);
  }

  @Test
  void aRoleOutsideTheBoundaryIsStillRefusedOnTheWriteDoor() {
    as("qits:reader")
        .contentType(ContentType.JSON)
        .body("{\"dryRun\":true}")
        .post(BASE + "/processes/gc/runs")
        .then()
        .statusCode(403);
  }
}
