package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.testdb.EmbeddedPg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * The surface of the <em>packaged artifact</em> — the fast-jar under {@code ./mvnw verify
 * -DskipITs=false}, the GraalVM binary under {@code -Dnative} — because that is where a whole class
 * of failure is visible and nowhere else.
 *
 * <p>Every other test here is a {@code @QuarkusTest}: it augments and runs in the build JVM, with
 * the full classpath present, reflection unrestricted, its datasource keys handed to it by a config
 * source and its peers replaced by an {@code @Alternative}. A native image has none of those. What
 * this asserts is exactly what that difference can lose:
 *
 * <ul>
 *   <li>the build-time route prefixes — {@code /orchestrator/api} and {@code /orchestrator/q} —
 *       which the edge path-routes verbatim on every host and no unprefixed form falls back to;
 *   <li>the shipped datasource <b>expression</b>: the launched process is handed {@code
 *       QITS_RESOURCE_DB_*}, the generic contract a deployment supplies, rather than the datasource
 *       keys, so the jar's own {@code ${…}} indirection is what is under test;
 *   <li>Flyway's migration surviving as a classpath resource, proven by reading a written row back
 *       over JDBC rather than through the API that wrote it;
 *   <li>every response type reaching Jackson through {@code Response.entity(...)}, which the
 *       build-time analysis cannot see — that is what {@link ApiWireReflection} is for, and a
 *       missing entry there is a 500 in the binary while the JVM suite stays green. The 202 from a
 *       started run is exactly such a response;
 *   <li><b>the client is served, and does not swallow the API.</b> Quinoa is disabled by default in
 *       test mode, so no {@code @QuarkusTest} builds or serves the SPA and every assertion about
 *       {@code /} would pass against a process with no client in it.
 * </ul>
 *
 * <p><b>This is also the only place the identity contract is real.</b> A {@code @QuarkusTest} runs
 * under the {@code test} profile, where qits-auth-core ships a dev user; the launched artifact runs
 * as a deployment does, so the roles have to arrive the way qits-gateway sends them — in {@code
 * X-Qits-User} and {@code X-Qits-Roles}. A request with neither is asserted to be refused.
 *
 * <p><b>And the peers here are REAL calls to a port nothing listens on.</b> The profile points every
 * target at a dead loopback address, so the run this launches fails every step in milliseconds —
 * which is the honest end-to-end proof that the executor, the store and the API carry a failure all
 * the way to a readable row without any of the suite's fakes involved.
 *
 * <p>ITs are skipped by default ({@code skipITs} in the root pom) because they need a {@code
 * package} to have happened. Ask for them explicitly.
 */
@QuarkusIntegrationTest
@TestProfile(PackagedSurfaceIT.PackagedUnderTarget.class)
public class PackagedSurfaceIT {

  /** The database this IT hands the launched process, on a name of its own. */
  private static final String DATABASE = "orchestrator_packaged_it";

  /**
   * The event bus's outbox, on a name of its own — and NOT overridden by the story profile below,
   * unlike the run log. Nothing writes to it in either suite (the bus is dark in both), so one
   * database serves them; what must not be shared is the run log, which both suites assert against.
   */
  private static final String EVENTSTREAM_DATABASE = "orchestrator_it_eventstream";

  /**
   * The one string that identifies a response as the CLIENT's index.html rather than anything else
   * this process serves. It is also the string that has to agree with {@code
   * quarkus.quinoa.ui-root-path} here and with {@code baseHref} in qits-platform-spa-orchestrator's
   * angular.json, so the probes below double as the check that all three still do. Both are the ROOT
   * now: this service has a host of its own and serves its client there.
   */
  private static final String BASE_HREF = "<base href=\"/\">";

  /**
   * Hands the launched artifact a database the way a deployment does — as the generic resource
   * triple, not as the datasource keys — and points every peer at a port nothing listens on.
   *
   * <p>The url travels through a system property rather than a static field: a test profile is
   * instantiated in more than one classloader, so a field written by one copy is not the field the
   * other reads, while the process has exactly one property table.
   */
  public static class PackagedUnderTarget implements QuarkusTestProfile {

    private static final String URL_PROPERTY = "qits.test.packaged-surface-it.db-url";

    private static final String EVENTSTREAM_URL_PROPERTY =
        "qits.test.packaged-surface-it.eventstream-db-url";

    @Override
    public Map<String, String> getConfigOverrides() {
      String dead = "http://127.0.0.1:" + deadPort();
      // A mutable map rather than Map.of: EVERY peer belongs on the dead port, and there are more
      // of them than that factory takes pairs. A peer missing from this list would be left on its
      // shipped alias, so "every peer fails" would be true only by accident of that name not
      // resolving in the step container.
      Map<String, String> overrides = new java.util.LinkedHashMap<>();
      overrides.put("QITS_RESOURCE_DB_URL", databaseUrl(URL_PROPERTY, DATABASE));
      overrides.put("QITS_RESOURCE_DB_USERNAME", EmbeddedPg.USER);
      overrides.put("QITS_RESOURCE_DB_PASSWORD", EmbeddedPg.PASSWORD);
      // The qits-eventstream jar's outbox. A launched process runs in NORMAL mode, so the %test
      // darkening in application.properties does not reach it and the datasource is opened and
      // migrated exactly as a deployment's is — which is the point: a process handed no eventstream
      // url dies at Flyway naming the missing variable, and that is the second half of the resource
      // contract .config/qits/deployments.yml declares.
      overrides.put(
          "QITS_RESOURCE_EVENTSTREAM_URL",
          databaseUrl(EVENTSTREAM_URL_PROPERTY, EVENTSTREAM_DATABASE));
      overrides.put("QITS_RESOURCE_EVENTSTREAM_USERNAME", EmbeddedPg.USER);
      overrides.put("QITS_RESOURCE_EVENTSTREAM_PASSWORD", EmbeddedPg.PASSWORD);
      // …and the bus itself stays dark, for the same reason every peer is on a dead port: there is
      // no qits-events here, and enabled the subscriber would redial a stream nobody serves for the
      // whole of the suite. It is a runtime key, so a launched process takes it.
      overrides.put("qits.eventstream.enabled", "false");
      overrides.put("qits.orchestrator.targets.artifacts-url", dead);
      overrides.put("qits.orchestrator.targets.containers-url", dead);
      overrides.put("qits.orchestrator.targets.ci-url", dead);
      overrides.put("qits.orchestrator.targets.deployments-url", dead);
      // projects and workspaces are the merged-branch sweep's two peers (2026-08-25).
      overrides.put("qits.orchestrator.targets.projects-url", dead);
      overrides.put("qits.orchestrator.targets.workspaces-url", dead);
      // maintenance and configuration are the two pin sources the registry keeps against
      // (2026-09-04): what repositories' mains reference, and what a launch would pull.
      overrides.put("qits.orchestrator.targets.maintenance-url", dead);
      overrides.put("qits.orchestrator.targets.configuration-url", dead);
      // idp is the service-client sweep's peer (qits-878).
      overrides.put("qits.orchestrator.targets.idp-url", dead);
      // The clock must not start a run beside the one this IT starts itself.
      overrides.put("quarkus.scheduler.enabled", "false");
      return overrides;
    }

    /**
     * The parking trick itself, {@code protected} and parameterised so a subclass in another
     * package can reuse it rather than copy it.
     *
     * <p>{@code stories.support.StoryProfile} needs a database of its OWN — the story catalogue
     * starts four gc runs and reads the history back, and sharing a database with this IT would
     * make each suite's assertions depend on whether the other had run. What it must not have of
     * its own is a second copy of the two-classloader workaround, which is the thing that is easy
     * to get subtly wrong; so the name is the subclass's and the mechanism stays here.
     */
    protected static synchronized String databaseUrl(String property, String database) {
      String recorded = System.getProperty(property);
      if (recorded != null) {
        return recorded;
      }
      // localhost resolves for the launched process too — it is a child of this JVM on this host.
      String url = EmbeddedPg.url(database);
      System.setProperty(property, url);
      return url;
    }

    /** A port taken and released, so a connection to it is refused rather than hanging. */
    private static int deadPort() {
      try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
        return socket.getLocalPort();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** What qits-gateway asserts for an authenticated operator. */
  private static RequestSpecification asAdmin() {
    return given().header("X-Qits-User", "packaged-it").header("X-Qits-Roles", "qits:admin");
  }

  @Test
  public void aRunStartsAsA202AndItsFailureIsCarriedAllTheWayIntoAReadableRow() {
    String id =
        asAdmin()
            .contentType(ContentType.JSON)
            .body("{\"dryRun\":false}")
            .when()
            .post("/orchestrator/api/processes/gc/runs")
            .then()
            // The 202 body goes out through Response.entity(...), which the native analysis cannot
            // see: a missing ApiWireReflection entry is a 500 here and green everywhere else.
            .statusCode(202)
            .contentType(ContentType.JSON)
            .body("id", Matchers.notNullValue())
            .extract()
            .path("id");

    String status = awaitClosed(id);
    // Every peer is a dead port, so every step that calls one fails; the run is FAILED and the
    // reason is a sentence rather than a stack trace.
    org.junit.jupiter.api.Assertions.assertEquals("FAILED", status);

    asAdmin()
        .when()
        .get("/orchestrator/api/runs/" + id)
        .then()
        .statusCode(200)
        // The whole gc plan, one row per StepDefinition in GcProcess.steps() — twenty-one since
        // the service-client sweep was added (qits-878), the same count ProcessApiTest and the
        // userflow IT pin.
        // Step 0 is the first peer read and fails against a dead port; step 8 (the registry plan)
        // depends on all six pin reads and is skipped fail-closed when they do.
        .body("steps.size()", Matchers.equalTo(21))
        .body("steps[0].status", Matchers.equalTo("FAILED"))
        .body("steps[0].error", Matchers.containsString("could not be called"))
        .body("steps[8].status", Matchers.equalTo("SKIPPED"))
        .body("steps[8].error", Matchers.containsString("skipped:"));

    // The round trip above would look identical against any database at all, so read the rows back
    // out of the postgres this JVM handed the process through ${QITS_RESOURCE_DB_URL}. That is the
    // whole claim: the shipped expression resolved, and Flyway's migration survived as a classpath
    // resource — exactly the shape a native image drops.
    assertTrue(stepRows(id) == 21, "the packaged process must have written its steps");
  }

  @Test
  public void thereIsNoAnonymousSurface() {
    given().when().get("/orchestrator/api/processes").then().statusCode(401);
  }

  @Test
  public void theMachineRoutesAreUnderTheSegmentAndAMistypedOneAnswersNoData() {
    asAdmin().when().get("/orchestrator/api/processes").then().statusCode(200);

    // The edge path-routes verbatim, so there is no unprefixed form to fall back to. The client sits
    // at the root now, so an unprefixed path is inside the SPA fallback's reach and comes back as
    // the page rather than as a 404 — which is right: /api belongs to no machine surface here.
    asAdmin()
        .when()
        .get("/api/processes")
        .then()
        .statusCode(200)
        .body(Matchers.containsString(BASE_HREF));

    String body =
        asAdmin().when().get("/orchestrator/api/nope").then().statusCode(404).extract().asString();
    assertFalse(body.contains("\"steps\""), "a mistyped path must not answer with data: " + body);
  }

  /**
   * The client is mounted at the root, and its {@code <base href>} agrees with where it is mounted.
   * The two are configured in different repositories — {@code quarkus.quinoa.ui-root-path} here, {@code
   * baseHref} in qits-platform-spa-orchestrator's angular.json — and a disagreement serves a page
   * that loads and then fetches its own JavaScript from a path that 404s. Nothing on this side
   * notices, which is why the string is asserted rather than the status alone.
   *
   * <p><b>It answers anonymously, and that is not a hole in "no anonymous surface".</b> That rule is
   * about this service's DATA: every route in {@link ProcessController} and {@link RunController} is
   * {@code @RolesAllowed} and the test above pins a 401 for an unauthenticated read. What is served
   * here is a static bundle with no configuration in it.
   */
  @Test
  public void theClientIsServedAtTheRootWithABaseHrefThatMatches() {
    given()
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .contentType(ContentType.HTML)
        .body(Matchers.containsString(BASE_HREF));
  }

  /**
   * A deep link is the SPA fallback doing its job: {@code /processes/gc} has no file behind it, and
   * {@code enable-spa-routing} is what makes a reload or a pasted link reach the Angular router
   * instead of a 404. An operator shares exactly these addresses.
   */
  @Test
  public void aDeepLinkFallsBackToTheClientSoTheAngularRouterOwnsIt() {
    given()
        .when()
        .get("/processes/gc")
        .then()
        .statusCode(200)
        .contentType(ContentType.HTML)
        .body(Matchers.containsString(BASE_HREF));
  }

  /**
   * THE HALF THAT COSTS SOMETHING IF IT IS WRONG. The SPA fallback is a late-order catch-all over
   * the WHOLE root now, so any path that matches no route is rerouted to index.html and answers
   * {@code 200 text/html} — unless {@code quarkus.quinoa.ignored-path-prefixes} claims it first. Its
   * one entry, {@code /orchestrator}, covers both machine prefixes: matching is by path segment.
   *
   * <p>The stake here is the client's own polling: {@code GET /orchestrator/api/runs/<id>} every two
   * seconds, which would hand a JSON parser an HTML document.
   *
   * <p><b>What is asserted is the status and the absence of the client's page — not the absence of
   * HTML.</b> An ignored path falls to Quarkus' own not-found handler, which answers {@code 404
   * text/html}: a correct refusal wearing a browser's content type.
   *
   * <p>Each entry in the list gets a case here. Add a literal route, add its prefix entry, add its
   * line below — the same commit.
   */
  @Test
  public void aMistypedMachinePathIs404AndNeverThePage() {
    asAdmin()
        .when()
        .get("/orchestrator/api/nope")
        .then()
        .statusCode(404)
        .body(Matchers.not(Matchers.containsString(BASE_HREF)));

    given()
        .when()
        .get("/orchestrator/q/nope")
        .then()
        .statusCode(404)
        .body(Matchers.not(Matchers.containsString(BASE_HREF)));
  }

  /**
   * THE BARE SEGMENT IS A MACHINE PATH AND ANSWERS LIKE ONE. {@code /orchestrator} is claimed by
   * {@code ignored-path-prefixes}, so it never becomes the page; it belongs to no route either, so it
   * is a 404. The old trailing-slash wart went with the move to the root — {@code /} is the client
   * now, and there is no bare segment left for a reader to mistype.
   */
  @Test
  public void theBareSegmentIsAMachinePathAndIsA404NeverThePage() {
    given()
        .when()
        .get("/orchestrator")
        .then()
        .statusCode(404)
        .body(Matchers.not(Matchers.containsString(BASE_HREF)));

    // The old client address, which nothing serves any more. It is claimed by the same entry, so a
    // stale bookmark gets a 404 rather than a page that would then fetch its assets from /.
    given()
        .when()
        .get("/orchestrator/")
        .then()
        .statusCode(404)
        .body(Matchers.not(Matchers.containsString(BASE_HREF)));
  }

  @Test
  public void theReadinessEndpointIsWhereTheDeploymentLooksForIt() {
    given()
        .when()
        .get("/orchestrator/q/health/ready")
        .then()
        .statusCode(200)
        .body("status", Matchers.equalTo("UP"));
  }

  @Test
  public void theApiDocumentAndItsUiAreServedUnderTheSegment() {
    // Both live under quarkus.http.non-application-root-path, which sits OUTSIDE quarkus.rest.path
    // and carries /orchestrator on its own; at / they would be unreachable through qits-gateway.
    given().when().get("/orchestrator/q/openapi").then().statusCode(200);
    given().when().get("/orchestrator/q/swagger-ui/").then().statusCode(200);
  }

  private static String awaitClosed(String id) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      String status =
          asAdmin()
              .when()
              .get("/orchestrator/api/runs/" + id)
              .then()
              .statusCode(200)
              .extract()
              .path("status");
      if (!"RUNNING".equals(status)) {
        return status;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("run " + id + " never finished");
  }

  private static int stepRows(String runId) {
    String url = EmbeddedPg.url(DATABASE);
    try (Connection connection =
            DriverManager.getConnection(url, EmbeddedPg.USER, EmbeddedPg.PASSWORD);
        PreparedStatement query =
            connection.prepareStatement("select count(*) from op_step where run_id = ?::uuid")) {
      query.setString(1, runId);
      try (ResultSet found = query.executeQuery()) {
        found.next();
        return found.getInt(1);
      }
    } catch (Exception e) {
      throw new IllegalStateException("could not read the resource database back", e);
    }
  }
}
