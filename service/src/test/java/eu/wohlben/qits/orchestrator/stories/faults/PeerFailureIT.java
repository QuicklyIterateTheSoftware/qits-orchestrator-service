package eu.wohlben.qits.orchestrator.stories.faults;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.stories.collection.GarbageCollectionRunIT;
import eu.wohlben.qits.orchestrator.stories.support.StoryIdentities;
import eu.wohlben.qits.orchestrator.stories.support.StoryNetwork;
import eu.wohlben.qits.orchestrator.stories.support.StoryPeers;
import eu.wohlben.qits.orchestrator.stories.support.StoryProfile;
import eu.wohlben.qits.orchestrator.stories.support.StoryRuns;
import eu.wohlben.qits.orchestrator.stories.support.StoryTarget;
import eu.wohlben.qits.userflows.Interactions;
import eu.wohlben.qits.userflows.NetworkCapture;
import eu.wohlben.qits.userflows.NetworkEdge;
import eu.wohlben.qits.userflows.UserStory;
import eu.wohlben.qits.userflows.UserStoryDescription;
import eu.wohlben.qits.userflows.UserflowRunsAfter;
import eu.wohlben.qits.userflows.report.ReportAssertions;
import eu.wohlben.qits.userflows.report.Slugs;
import eu.wohlben.qits.userflows.report.UserflowReport;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/**
 * <b>A night one peer is down</b> — and the rule that decides what still happens.
 *
 * <p>"Fail-closed is an edge, not an {@code if}" is the sentence this repository's working notes
 * open the executor section with, and it is the hardest thing here to see from the outside. Nothing
 * deletes against a keep-set it could not read, and the mechanism is the DEPENDENCY rather than a
 * check inside a step: every step that deletes declares all six pin reads as edges, so a failed pin
 * read skips every one of them before a body runs (ticket qits-1175). The empty-keep-set path in
 * {@code GcProcess.imagesBody} is kept as a belt and never gets the chance.
 *
 * <p><b>The rule covers every store, not only the ones the broken source protects.</b> Before
 * qits-1175 the volume sweep, the build-cache prune, the branch sweep and the rest ran on a night a
 * pin read failed, on the reasoning that they had no keep-set to lose. A cut-off pin answer that
 * read as success showed what that reasoning costs: one bad read, and the only thing between the
 * platform and a wrong delete was a guard inside one peer. Now a run with an unread pin source
 * deletes nothing.
 *
 * <p>So one story, one broken peer, and a diagram that says it: an arrow to qits-ci carrying a 503,
 * arrows for the reads that answered anyway, and <b>no arrow to any deleter</b>. The registry and
 * the host ARE reached, once each, by the measurements that need no pin, so the honest claim is
 * about which calls are missing rather than about which peer is untouched.
 *
 * <h2>How the peer is broken</h2>
 *
 * <p>{@link StoryPeers#refuse} is the one piece of state in the stand-in, and the class javadoc over
 * there says why it has to be state here and can be a path elsewhere: a gc run's nineteen paths are
 * fixed by {@code GcProcess.steps()} and identical in every run, so "qits-ci is down tonight" cannot be
 * spelled as a url the story addresses. It is armed inside a {@code try} and cleared in a {@code
 * finally}, and cleared again in {@code @AfterEach} — a refusal that outlived its story would be a
 * broken peer in somebody else's diagram, and the two would look exactly alike.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
public class PeerFailureIT {

  static final String CATEGORY = "resilience";

  static final String CATEGORY_SLUG = Slugs.slug(CATEGORY);

  static final String FAIL_CLOSED =
      "A pin nobody could read deletes nothing in any store";

  static final String FAIL_CLOSED_SLUG = Slugs.slug(FAIL_CLOSED);

  static final String CLAIMS_UNREAD =
      "A claim set nobody could read removes no service client";

  static final String CLAIMS_UNREAD_SLUG = Slugs.slug(CLAIMS_UNREAD);

  @BeforeAll
  static void tapBothEndsOfTheNetwork() {
    StoryNetwork.install();
  }

  /** Belt for the {@code finally} below: no other story may inherit a broken peer. */
  @AfterEach
  void everyPeerAnswersAgain() {
    StoryPeers.answerNormally();
  }

  private static Supplier<RequestSpecification> operator() {
    return () -> StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT);
  }

  @UserStory(value = FAIL_CLOSED, category = CATEGORY)
  @UserStoryDescription(
      """
      qits-ci is being redeployed at three in the morning, so the read that says which daemon binary
      must survive the night answers 503. Every step that deletes is skipped before its body runs —
      the registry plan and sweep, host images, orphan volumes, the build cache, merged branches,
      retired configuration entries, decommissioned tags and unclaimed service clients — and the run
      is FAILED, because a peer that could not be read is a failure and not a footnote.

      The reads still happen. The disk is still measured and so is the registry store, the five pin
      reads that answered are still read, and so are the repository catalogue and the service-client
      claims, so the night's record still says what the platform held. A run that cannot read its
      whole keep-set deletes nothing, in any store: one night of no reclaim is cheap, and a wrong
      delete is not.

      A skipped step names the step that actually FAILED rather than the skipped neighbour in
      between, so a reader does not have to walk the graph backwards to find the cause. And the
      whole of what "fail-closed" means is visible on the diagram rather than described: one arrow
      to qits-ci carrying its 503, nine arrows for the reads that answered, and not one arrow to a
      deleter.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  void aBrokenPinReadSkipsEveryDeleter(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    String id;
    JsonPath run;
    StoryPeers.refuse(StoryPeers.DAEMON_PATH);
    try {
      story.note("qits-ci is being redeployed; its daemon pin answers 503").as("peer-is-down");
      id = StoryRuns.start(operator(), false);
      run = StoryRuns.detail(operator(), id);
    } finally {
      // Always, and before any assertion: a refusal that outlived this story would be a broken peer
      // in the next one's diagram.
      StoryPeers.answerNormally();
    }

    assertEquals("FAILED", run.getString("status"), "a broken peer must fail the run: " + run.prettify());
    story
        .note(
            "the run is FAILED — a peer that could not be read is a failure, not a footnote — and"
                + " the failure is a sentence in a row rather than a stack trace")
        .as("run-failed");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body(StoryRuns.stepPath("pins.ci") + ".status", equalTo("FAILED"))
        .body(StoryRuns.stepPath("pins.ci") + ".httpStatus", equalTo(StoryPeers.REFUSED_STATUS))
        .body(StoryRuns.stepPath("pins.ci") + ".error", containsString("answered 503"))
        // Everything that would delete on the strength of that pin, skipped before a body ran —
        // and each one naming the step that actually failed rather than its neighbour.
        .body(StoryRuns.stepPath("artifacts.plan") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("artifacts.plan") + ".error", equalTo("skipped: pins.ci failed"))
        .body(StoryRuns.stepPath("artifacts.sweep") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("artifacts.sweep") + ".error", equalTo("skipped: pins.ci failed"))
        // tags.sweep carries all six pin edges too, so the same broken read skips it before its
        // body — carrying every pin, not just pins.deployments — is ever built.
        .body(StoryRuns.stepPath("tags.sweep") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("tags.sweep") + ".error", equalTo("skipped: pins.ci failed"));
    story
        .note(
            "the registry plan and the sweep behind it are SKIPPED before either body runs — the"
                + " edge is the guarantee — and both name pins.ci, the step that actually failed,"
                + " rather than the skipped neighbour in between")
        .as("fail-closed-cascade");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        // The reads still run: the other five pins, the catalogue, the claims, the measurements.
        .body(StoryRuns.stepPath("pins.deployments") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.dependencies") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.images") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.workspaces") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.projects") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("usage.before") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("artifacts.usage.before") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("repos.catalogue") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("claims.idp-clients") + ".status", equalTo("SUCCEEDED"))
        // Every other deleter, in every store, skipped naming the read that failed (qits-1175).
        .body(StoryRuns.stepPath("containers.images") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("containers.volumes") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("containers.build-cache") + ".status", equalTo("SKIPPED"))
        .body(
            StoryRuns.stepPath("containers.build-cache") + ".error",
            equalTo("skipped: pins.ci failed"))
        .body(StoryRuns.stepPath("branches.sweep") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("configuration.entries") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("idp.service-clients") + ".status", equalTo("SKIPPED"));
    story
        .note(
            "the reads still ran — five pins, the catalogue, the claims and both opening"
                + " measurements — and every other deleter was skipped too: host images, volumes,"
                + " build cache, branches, configuration entries and service clients")
        .as("every-deleter-skipped");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        // usage.after waits on the registry sweep, which was skipped BECAUSE something failed — so
        // this skip cascades, and it too names the origin.
        .body(StoryRuns.stepPath("usage.after") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("usage.after") + ".error", equalTo("skipped: pins.ci failed"))
        .body(StoryRuns.stepPath("artifacts.usage.after") + ".status", equalTo("SKIPPED"))
        .body(
            StoryRuns.stepPath("artifacts.usage.after") + ".error",
            equalTo("skipped: pins.ci failed"))
        .body("summary", containsString("pins.ci FAILED"));
    story
        .note(
            "the closing measurement is skipped too, and by the same origin: a FAILURE skip is"
                + " contagious where the dry run's POLICY skip was not, which is the distinction"
                + " that cost this service a green run reading as broken before it existed")
        .as("failure-skip-cascades");
  }

  @UserStory(value = CLAIMS_UNREAD, category = CATEGORY)
  @UserStoryDescription(
      """
      qits-deployments is the one service that knows which service clients are still claimed — every
      application that declares an idp:client resource holds one — and tonight its claims read
      answers 503. A claim set nobody could read is not an empty claim set: to qits-idp an empty
      one would mean nothing is claimed, and every service client on the platform, this
      orchestrator's own included, would be condemned.

      So the sweep is skipped before its body runs, naming the read that failed, and qits-idp is
      not asked for anything at all — the diagram has no arrow to it. The run is FAILED, because a
      peer that could not be read is a failure. Everything else the night does needs no claim and
      runs as it always does: the pins are read, the registry is planned and swept, the host is
      pruned, branches and tags are swept and both stores are measured again.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  void anUnreadClaimSetSkipsTheServiceClientSweepAndAsksQitsIdpForNothing(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    String id;
    JsonPath run;
    StoryPeers.refuse(StoryPeers.CLAIMS_PATH);
    try {
      story
          .note("qits-deployments' idp-client claims answer 503 tonight")
          .as("claims-are-down");
      id = StoryRuns.start(operator(), false);
      run = StoryRuns.detail(operator(), id);
    } finally {
      StoryPeers.answerNormally();
    }

    assertEquals(
        "FAILED", run.getString("status"), "an unread claim set must fail the run: " + run.prettify());
    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body(StoryRuns.stepPath("claims.idp-clients") + ".status", equalTo("FAILED"))
        .body(
            StoryRuns.stepPath("claims.idp-clients") + ".httpStatus",
            equalTo(StoryPeers.REFUSED_STATUS))
        .body(StoryRuns.stepPath("idp.service-clients") + ".status", equalTo("SKIPPED"))
        .body(
            StoryRuns.stepPath("idp.service-clients") + ".error",
            equalTo("skipped: claims.idp-clients failed"))
        .body(StoryRuns.stepPath("idp.service-clients") + ".request", nullValue());
    story
        .note(
            "the service-client sweep is SKIPPED before its body runs, naming claims.idp-clients —"
                + " and it made no request: an unread claim set is never sent as an empty one")
        .as("sweep-skipped");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body(StoryRuns.stepPath("artifacts.sweep") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("tags.sweep") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("configuration.entries") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("usage.after") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("artifacts.usage.after") + ".status", equalTo("SUCCEEDED"));
    story
        .note(
            "nothing else waits on a claim: the registry, the host, branches and tags are all"
                + " collected and both stores measured again")
        .as("rest-of-the-night-runs");
  }

  @AfterAll
  static void theFailClosedStoryIsComplete() {
    ReportAssertions.assertComplete(CATEGORY_SLUG, FAIL_CLOSED_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of(
            "peer-is-down",
            "run-failed",
            "fail-closed-cascade",
            "every-deleter-skipped",
            "failure-skip-cascades")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, FAIL_CLOSED_SLUG, step);
    }

    from(StoryIdentities.OPERATOR, "POST " + StoryTarget.GC_RUNS_PATH + " -> 202");
    from(StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");

    // The broken peer, drawn with the status it answered — evidence rather than a claim.
    to(StoryPeers.CI, StoryPeers.label("GET", StoryPeers.DAEMON_PATH, StoryPeers.REFUSED_STATUS));
    // …and the nine reads that happened anyway. Each store was measured once rather than twice,
    // because both after-steps were skipped.
    to(StoryPeers.CONTAINERS, StoryPeers.read(StoryPeers.USAGE_PATH));
    to(StoryPeers.ARTIFACTS, StoryPeers.read(StoryPeers.STORE_PATH));
    to(StoryPeers.MAINTENANCE, StoryPeers.read(StoryPeers.DEPENDENCY_PINS_PATH));
    to(StoryPeers.CONFIGURATION, StoryPeers.read(StoryPeers.IMAGE_PINS_PATH));
    to(StoryPeers.WORKSPACES, StoryPeers.read(StoryPeers.WORKSPACE_LAUNCH_PINS_PATH));
    to(StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.PROJECT_LAUNCH_PINS_PATH));
    to(StoryPeers.DEPLOYMENTS, StoryPeers.read(StoryPeers.PINS_PATH));
    to(StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.REPOSITORIES_PATH));
    to(StoryPeers.DEPLOYMENTS, StoryPeers.read(StoryPeers.CLAIMS_PATH));

    // THE CLAIM A PRESENCE CHECK CANNOT MAKE. Nothing that DELETES was called, in any store, because
    // a pin source could not be read. Every deleter's path has `/gc/` in it except the usage read,
    // which is a measurement; the registry and the host are reached for their size only.
    UserflowReport report = ReportAssertions.read(CATEGORY_SLUG, FAIL_CLOSED_SLUG);
    assertTrue(
        report.network().stream()
            .noneMatch(
                edge ->
                    StoryTarget.SERVICE.equals(edge.from())
                        && edge.label().contains("/gc/")
                        && !edge.label().contains(StoryPeers.USAGE_PATH)),
        () -> "a collection call was made without every pin: " + report.network());
    assertTrue(
        report.network().stream().noneMatch(edge -> StoryPeers.IDP.equals(edge.to())),
        () -> "qits-idp was asked to delete without every pin: " + report.network());
    // Two in, ten out. The credential was minted an hour ago by the first run of the
    // catalogue, so no token arrow belongs here — see StoryPeers on why exactly one story owns that
    // edge.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, FAIL_CLOSED_SLUG, 12);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG,
        FAIL_CLOSED_SLUG,
        List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));

    // --- the unread claim set ------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, CLAIMS_UNREAD_SLUG, UserflowReport.PASSED);
    for (String step : List.of("claims-are-down", "sweep-skipped", "rest-of-the-night-runs")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, CLAIMS_UNREAD_SLUG, step);
    }
    ReportAssertions.assertEdge(
        CATEGORY_SLUG,
        CLAIMS_UNREAD_SLUG,
        NetworkEdge.HTTP,
        StoryTarget.SERVICE,
        StoryPeers.DEPLOYMENTS,
        StoryPeers.label("GET", StoryPeers.CLAIMS_PATH, StoryPeers.REFUSED_STATUS));
    // THE CLAIM. Not one request reached qits-idp — no sweep, and no token either: the credential
    // was minted by the first run of the catalogue and is cached for the hour.
    ReportAssertions.assertNoEdgesTo(CATEGORY_SLUG, CLAIMS_UNREAD_SLUG, StoryPeers.IDP);
    // Two in, eighteen out: the seventeen calls that need no claim, the claims read as its 503 —
    // and no service-client sweep.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, CLAIMS_UNREAD_SLUG, 20);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG,
        CLAIMS_UNREAD_SLUG,
        List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));
  }

  private static void from(String actor, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, FAIL_CLOSED_SLUG, NetworkEdge.HTTP, actor, StoryTarget.SERVICE, label);
  }

  private static void to(String peer, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, FAIL_CLOSED_SLUG, NetworkEdge.HTTP, StoryTarget.SERVICE, peer, label);
  }
}
