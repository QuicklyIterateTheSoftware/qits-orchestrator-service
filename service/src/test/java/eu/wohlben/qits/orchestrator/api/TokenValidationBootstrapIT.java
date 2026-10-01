package eu.wohlben.qits.orchestrator.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.stories.support.StoryIdentities;
import eu.wohlben.qits.orchestrator.stories.support.StoryNetwork;
import eu.wohlben.qits.orchestrator.stories.support.StoryProfile;
import eu.wohlben.qits.servicemock.idp.MockIdp;
import eu.wohlben.qits.userflows.Interactions;
import eu.wohlben.qits.userflows.NetworkCapture;
import eu.wohlben.qits.userflows.UserStory;
import eu.wohlben.qits.userflows.UserStoryDescription;
import eu.wohlben.qits.userflows.report.ReportAssertions;
import eu.wohlben.qits.userflows.report.UserflowReport;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * The whole service as it is <b>packaged</b> — like {@link PackagedSurfaceIT} beside it, but with
 * the OIDC tenant <b>on</b>, which no {@code @QuarkusTest} here can prove.
 *
 * <p>The shipped tenant is {@code quarkus.oidc.tenant-enabled=${qits.auth.machine.required:false}},
 * and <b>no suite in this repository turns that gate on except the story catalogue</b> — the test
 * application.properties says so in as many words ("the OIDC tenant is disabled because
 * qits.auth.machine.required defaults false"), which is what keeps a clone-alone {@code ./mvnw
 * verify} free of an issuer. Every {@code @QuarkusTest} here therefore runs under the {@code test}
 * profile's dev user, and {@link PackagedSurfaceIT} proves only the OTHER track of identity — the
 * forward-auth headers qits-gateway asserts. The consequence is that the entire shipped {@code
 * quarkus.oidc.*} block — auth-server-url with {@code discovery-enabled=false} and {@code
 * jwks-path=jwks} joined onto it, the boot-time fetch that {@code connection-delay} retries,
 * audience enforcement, groups&rarr;roles mapping — is exercised NOWHERE else. This is where it
 * starts. The far side is {@link MockIdp}, whose recordings make the interaction assertable on
 * <b>both ends</b>.
 *
 * <p><b>This is the catalogue's first story class and it owns the boot.</b> Every story class in
 * this repository names {@link StoryProfile}, which is what makes them one launched process rather
 * than several; and {@code UserflowClassOrderer} sorts by fully-qualified class name, so {@code
 * …orchestrator.api} runs before {@code …orchestrator.stories.*}. That matters because a cumulative
 * source is attributed by a cursor: traffic that happened before any story ran — the startup JWKS
 * fetch, which is the whole subject of the first story here — lands in whichever story drains
 * <i>first</i>. Pinning both orders is what keeps that the story it belongs to.
 *
 * <p>The proof doubles as documentation, emitted under {@code target/userstories/} with a network
 * diagram beside the steps. The diagram is <b>observed, never narrated</b> — the framework's
 * shipped {@code NetworkTaps.restAssured} sees what a story sends into this service, {@link MockIdp}
 * and the peer stand-in supply what this service sent out, and the framework drains all three at
 * story end. (This repository carried a hand-copied {@code StoryNetworkFilter} for exactly the
 * first of those; it was deleted when the rest of the catalogue was written.) A story method
 * therefore asserts and notes; it draws nothing, which is also why the plan assertion below is a
 * note: it is a claim about the BODY of an answer already on the diagram, not a second request. The
 * stories are browserless (no {@code Flow} parameter), so no Chromium is involved anywhere.
 *
 * <p><b>The route both stories drive is {@code GET /orchestrator/api/processes}</b>, the process
 * catalogue, and it is chosen as the least side-effectful read this service has. Every route here
 * is {@code @RolesAllowed({"qits:admin", "qits:system"})}, so the machine role reaches all of them
 * and the choice is about what the request DOES, not about what it takes:
 *
 * <ul>
 *   <li>it names nothing. It is the only guarded read with no path parameter — the run listing
 *       carries a process kind, the run detail a run id;
 *   <li>it opens no store and calls no peer. {@code Runs.processes()} reads the {@code
 *       ProcessRegistry}, which is CDI discovery and nothing else, so the answer is a fact about
 *       THIS artifact rather than about a peer;
 *   <li>and it is the read whose answer is the pinned contract. {@code qits-orchestrator-plan.md}
 *       in the qits-qits wrapper fixes the step ids and the edges between them and four
 *       repositories build against them, so what the catalogue serves is the plan itself.
 * </ul>
 *
 * <p><b>The write is a story too, and it is somebody else's.</b> {@code POST /processes/{kind}/runs}
 * starts real deletions on six other services, so the stories that drive it live in {@code
 * stories.collection} beside a stand-in that answers as all six — and the refusals on that door,
 * which is where "no anonymous surface" actually costs something, live in {@code stories.refusals}.
 * Keeping them apart keeps this class about the one thing only a gate-on boot can show: that the
 * keys were fetched before any caller arrived.
 *
 * <p><b>ITs are skipped by default here and this one does NOT flip that.</b> {@code skipITs} is
 * {@code true} in the root pom because {@link PackagedSurfaceIT} is this module's other integration
 * test and a good half of it is about the CLIENT — the base href at the root, the deep links, the
 * fallback that must not swallow {@code /orchestrator} — which the userflow pipeline deliberately
 * does not build ({@code -Dquarkus.quinoa=false}, since the qits-platform-spa-orchestrator
 * submodule arrives EMPTY in a step container). A blanket {@code -DskipITs=false} would make that
 * run red on a test that is right. {@code .config/qits/ci-event-userflows.yml} names the story
 * classes instead, which is also what keeps the userflow pipeline about these stories and nothing
 * else — and keeps the property's own meaning ("run everything") intact for the {@code native}
 * profile in service/pom.xml that sets it.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TokenValidationBootstrapIT {

  static final String CATEGORY = "authentication";
  static final String ACCEPTED_SLUG =
      "on-start-the-orchestrator-fetches-the-platform-s-signing-keys";
  static final String DENIED_SLUG = "a-stranger-s-token-never-reads-the-platform-s-deletion-plan";

  private static final String PROCESSES = "/orchestrator/api/processes";

  /** How the diagram names this service on both sides of an edge. */
  static final String SERVICE = "qits-platform-orchestrator";

  /** The one process this platform has, and the kind every step id below hangs under. */
  private static final String GC = "find { it.kind == 'gc' }";

  /**
   * Wires every end of the network diagram, once, before either story runs.
   *
   * <p>{@link StoryNetwork} installs the framework's RestAssured tap (the near side), registers the
   * idp's recording as a <b>cumulative</b> source with no floor, and registers the peer stand-in's
   * access log. The idp source is the one that matters here: the supplier hands over the mock's
   * whole request log every time it is asked and the framework remembers how much of it earlier
   * stories already consumed, so the startup fetch — recorded long before any story existed — is
   * attributed to the first story and to that one only. It is invoked lazily at story end, so
   * registering it here is safe even though nothing has been recorded yet.
   *
   * <p>The label carries the status the mock <i>answered</i> with, which is the half a method and
   * path cannot supply: {@code "GET /idp/jwks -> 200"} is evidence that the keys were served, not
   * merely asked for.
   */
  @BeforeAll
  static void tapEveryEndOfTheNetwork() {
    StoryNetwork.install();
  }

  @UserStory(
      value = "On start, the orchestrator fetches the platform's signing keys",
      category = "authentication")
  @UserStoryDescription(
      """
      A freshly deployed qits-platform-orchestrator must validate service bearers before any
      caller arrives: at startup it fetches the signing keys (JWKS) from qits-platform-idp —
      discovery stays off, the path is configured — so the very first machine request is
      accepted. What that bearer then opens is the plan itself: the technical processes this
      service runs, their steps, and the dependency edges between them. That listing is the
      contract four repositories build against, and it is what a caller reads before it asks for
      a run — the run being minutes of somebody else's pruning, on four other services' stores.
      """)
  @Order(1)
  void serviceBootFetchesJwksAndAcceptsPlatformTokens(Interactions story) {
    MockIdp idp = MockIdp.attach();

    story.note(
        "qits-platform-orchestrator starts with the OIDC tenant on, beside a reachable"
            + " qits-platform-idp");
    given().get("/orchestrator/q/health/ready").then().statusCode(200);

    // End (a), the idp side: the JWKS was served during startup — before this story presented any
    // token at all. Readiness above is deliberately independent of that fetch — the shipped config
    // explains why: tying it to another service's would make a cold boot a question of ordering —
    // so a 200 there is not the claim. The recording is.
    assertTrue(
        idp.recordedRequests().stream().anyMatch(r -> "/idp/jwks".equals(r.path())),
        "the packaged service never fetched /idp/jwks at startup");
    story
        .note("the signing keys were fetched at startup, before this story presented any token")
        .as("jwks-fetched");

    // End (b), this service's side: those keys are what token validation now runs on. A platform
    // peer's bearer (aud = this service, roles in `groups`) opens the process catalogue — no path
    // parameter, nothing named, no store opened and no peer called.
    //
    // The actor is set BEFORE the call: the tap sees a request, never a narrative role, and this is
    // what makes the observed edge read `a platform service -> qits-platform-orchestrator`.
    NetworkCapture.actor("a platform service");
    String peerToken =
        idp.token()
            .subject("a-platform-service")
            .audience(StoryIdentities.AUDIENCE)
            .groups(StoryIdentities.MACHINE_ROLE)
            .mint();
    given()
        .header("Authorization", "Bearer " + peerToken)
        .get(PROCESSES)
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("kind", hasItem("gc"))
        .body(GC + ".name", equalTo("Garbage collection"))
        .body(GC + ".description", notNullValue());
    story
        .note("a platform peer's bearer (aud=qits-platform, groups=[qits:system]) is"
            + " accepted and the catalogue is served")
        .as("catalogue-served");

    // End (c), and it is what makes this answer worth guarding rather than a greeting: the plan the
    // catalogue serves is the pinned one. The step ids and their edges are fixed by
    // qits-orchestrator-plan.md and read by four repositories, and augmentation is where a process
    // bean could quietly fail to be discovered — ProcessRegistry finds them by CDI, so a fast-jar
    // that lost the bean would answer 200 with an empty list and nothing else here would notice.
    //
    // The dependency asserted is the fail-closed one and it is asserted BECAUSE it is a promise made
    // on the wire: `artifacts.plan` waits on both pin reads, so a pin read that failed skips the
    // plan, the sweep behind it and the image collection beside it before any body runs. A reader of
    // this document can see that without running a night — and `stories.faults` then shows it
    // happening.
    //
    // This is the same route, the same actor and the same status as the call above, so the tap
    // observes one arrow for both — which is right: what is new here is not a hop but a claim about
    // the BODY, and a claim about a body is a step rather than an edge. It is therefore a note.
    given()
        .header("Authorization", "Bearer " + peerToken)
        .get(PROCESSES)
        .then()
        .statusCode(200)
        .body(GC + ".steps.size()", equalTo(19))
        .body(GC + ".steps.id", hasItem("artifacts.sweep"))
        .body(
            GC + ".steps.find { it.id == 'artifacts.plan' }.dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.images",
                "pins.workspaces",
                "pins.projects"));
    story
        .note("the gc plan is the pinned one: 19 steps, artifacts.plan depends on all six pin reads"
            + " (fail-closed)")
        .as("plan-is-the-pinned-one");
  }

  @UserStory(
      value = "A stranger's token never reads the platform's deletion plan",
      category = "authentication")
  @UserStoryDescription(
      """
      The flip side of trusting the platform's keys. A request with no credential at all is
      refused, because there is no anonymous route in this service and there must never be one:
      what the surface behind it starts is a deletion run on four other services' stores. A
      token signed by a key the published JWKS never carried, or addressed to an audience this
      platform never mints, is refused the same way — however well-formed it looks: both are 401
      and not 403, because the credential never became an identity and there is no caller to have
      been forbidden. A token addressed here and signed correctly but carrying a role this service has
      never heard of gets the other answer, 403 — it authenticated and covers nothing.
      """)
  @Order(2)
  void aStrangersTokenIsRefused(Interactions story) {
    MockIdp idp = MockIdp.attach();

    // Three initiators in this story and three arrows, so the actor is set before each group rather
    // than once up front — nobody, then a forger, then a real caller holding the wrong role.
    NetworkCapture.actor("an unauthenticated caller");

    // The first door, and it is this service's own shape rather than the fleet's. The gate being on
    // changes nothing about it: with the tenant enabled there are two mechanisms at the door, the
    // bearer's and qits-gateway's forward-auth pair, and a request carrying neither satisfies
    // neither. PackagedSurfaceIT pins the same 401 with the gate OFF; the pair is what says the
    // refusal is not an artefact of how this service happens to be configured.
    given().get(PROCESSES).then().statusCode(401);
    story
        .note("a request carrying no credential at all satisfies neither mechanism and is refused")
        .as("anonymous-refused");

    NetworkCapture.actor("an impostor");
    String strangersToken =
        idp.token()
            .audience(StoryIdentities.AUDIENCE)
            .groups(StoryIdentities.MACHINE_ROLE)
            .signedByUnknownKey()
            .mint();
    given()
        .header("Authorization", "Bearer " + strangersToken)
        .get(PROCESSES)
        .then()
        .statusCode(401);
    // The impostor's two credentials are the same edge — same actor, same route, same status — so
    // the diagram draws one arrow and the notes are what keep them distinguishable. That is the
    // right division: the graph says who reached what and got what, the steps say why.
    story
        .note("a token signed by a key the published JWKS never carried is refused")
        .as("unknown-key-refused");

    String wrongAudienceToken =
        idp.token()
            .audience(StoryIdentities.OFF_PLATFORM_AUDIENCE)
            .groups(StoryIdentities.MACHINE_ROLE)
            .mint();
    given()
        .header("Authorization", "Bearer " + wrongAudienceToken)
        .get(PROCESSES)
        .then()
        .statusCode(401);
    story
        .note("a token addressed to an audience this platform never mints is refused just the same")
        .as("wrong-audience-refused");

    // The last door, and the one that proves the groups→roles mapping really ran rather than being
    // waved through: `qits:reader` is a real platform role and it is not one of the two this
    // service's routes name. Minted into a token addressed here it authenticates perfectly and
    // still covers nothing. The two that DO open this route — qits:admin and qits:system — are
    // deliberately not distinguished anywhere in this service, so there is no narrower ceiling to
    // draw: an operator presses Run now in a browser and a machine may post the same run.
    //
    // A real caller with a real credential, so a third actor and a third arrow: it authenticated,
    // which is exactly what the 403 rather than a 401 records.
    NetworkCapture.actor("a caller with the wrong role");
    String readerToken =
        idp.token()
            .subject("somebody-elses-service")
            .audience(StoryIdentities.AUDIENCE)
            .groups(StoryIdentities.UNPRIVILEGED_ROLE)
            .mint();
    given().header("Authorization", "Bearer " + readerToken).get(PROCESSES).then().statusCode(403);
    story
        .note("a token carrying a role this service never names authenticates and covers nothing")
        .as("wrong-role-refused");
  }

  @AfterAll
  static void bothStoryReportsAreComplete() {
    // The extension emits each report in its afterEach, so both are on disk before @AfterAll runs.
    // assertComplete now also proves the network section: the sidecar's edges are canonical, the
    // networkHash recomputes from them, and every mermaid line is in the markdown.
    ReportAssertions.assertComplete(CATEGORY, ACCEPTED_SLUG, UserflowReport.PASSED);
    // Observed on the far side, drained from the mock's recording, and attributed to this story
    // because it is the first one that ran (see the class javadoc on ordering).
    ReportAssertions.assertEdge(
        CATEGORY, ACCEPTED_SLUG, "http", SERVICE, MockIdp.SERVICE_NAME, "GET /idp/jwks -> 200");
    // Observed on the near side, by the shipped tap, with the actor this story set. Both reads of
    // the catalogue collapse into this one arrow, which is why the plan claim is a note.
    ReportAssertions.assertEdge(
        CATEGORY, ACCEPTED_SLUG, "http", "a platform service", SERVICE,
        "GET " + PROCESSES + " -> 200");
    ReportAssertions.assertStepId(CATEGORY, ACCEPTED_SLUG, "jwks-fetched");
    ReportAssertions.assertStepId(CATEGORY, ACCEPTED_SLUG, "catalogue-served");
    ReportAssertions.assertStepId(CATEGORY, ACCEPTED_SLUG, "plan-is-the-pinned-one");

    ReportAssertions.assertComplete(CATEGORY, DENIED_SLUG, UserflowReport.PASSED);
    // Three doors, three initiators, three arrows — the whole of what the denied story documents.
    ReportAssertions.assertEdge(
        CATEGORY, DENIED_SLUG, "http", "an unauthenticated caller", SERVICE,
        "GET " + PROCESSES + " -> 401");
    ReportAssertions.assertEdge(
        CATEGORY, DENIED_SLUG, "http", "an impostor", SERVICE, "GET " + PROCESSES + " -> 401");
    ReportAssertions.assertEdge(
        CATEGORY, DENIED_SLUG, "http", "a caller with the wrong role", SERVICE,
        "GET " + PROCESSES + " -> 403");
    ReportAssertions.assertStepId(CATEGORY, DENIED_SLUG, "anonymous-refused");
    ReportAssertions.assertStepId(CATEGORY, DENIED_SLUG, "unknown-key-refused");
    ReportAssertions.assertStepId(CATEGORY, DENIED_SLUG, "wrong-audience-refused");
    ReportAssertions.assertStepId(CATEGORY, DENIED_SLUG, "wrong-role-refused");
  }
}
