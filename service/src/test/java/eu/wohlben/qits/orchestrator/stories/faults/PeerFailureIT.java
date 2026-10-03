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
 * check inside a step: {@code artifacts.plan}, {@code artifacts.sweep} and {@code containers.images}
 * declare the pin reads as edges, so a failed pin read skips all three before a body runs. The
 * empty-keep-set path in {@code GcProcess.imagesBody} is kept as a belt and never gets the chance.
 *
 * <p><b>The rule cuts the other way too, and that half is the expensive one.</b> A step with no
 * keep-set must NOT wait on a pin: {@code containers.volumes} and {@code containers.build-cache}
 * hang off the disk measurement alone, because a prune and a dangling-volume sweep have nothing a
 * pin could protect — and the build cache is the larger half of the measured problem, so skipping it
 * on a pin failure would cost the platform the night's biggest reclaim for a reason that does not
 * apply to it.
 *
 * <p>So one story, one broken peer, and a diagram that says both halves at once: an arrow to
 * qits-ci carrying a 503, fifteen arrows to the peers that answered anyway, and <b>no arrow to
 * qits-artifacts that would have deleted anything</b> — the plan and the sweep are simply not there.
 * The registry IS reached, once, by the store measurement that needs no pin, so the honest claim is
 * about which calls are missing rather than about which peer is untouched; a presence check cannot
 * make either.
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
      "A pin nobody could read deletes nothing, and stops nothing that needs no pin";

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
      must survive the night answers 503. Everything that would delete on the strength of that pin
      is skipped before its body runs — the registry plan, the registry sweep — and the run is
      FAILED, because a peer that could not be read is a failure and not a footnote.

      Nothing else stops. The disk is still measured and so is the registry store, the five pin
      reads that answered are still read, host images are still collected against the deployment
      pins that DID answer, orphan volumes are still swept and the build cache is still pruned —
      which is the largest reclaim of the night and has no keep-set anybody could have protected it
      with. The repository catalogue is still read and merged branches are still swept. A night
      where one broken peer stopped every unrelated reclaim would be a night of no reclaim for no
      reason.

      A skipped step names the step that actually FAILED rather than the skipped neighbour in
      between, so a reader does not have to walk the graph backwards to find the cause. And the
      whole of what "fail-closed" means is visible on the diagram rather than described: one arrow
      to qits-ci carrying its 503, fifteen arrows to the peers that answered, and not one arrow to
      qits-artifacts that would have deleted anything — the registry is read for its size and asked
      for nothing else.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  void aBrokenPinReadSkipsOnlyWhatDeletesOnTheStrengthOfIt(Interactions story) {
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
        // The other pin answered, so the keep-set it protects exists and the image sweep runs.
        .body(StoryRuns.stepPath("pins.deployments") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("containers.images") + ".status", equalTo("SUCCEEDED"))
        // The other four pin reads answered as well, and they are the sources no deployment could
        // have stood in for: what repositories' mains reference, what the next deploy would
        // configure, and what each launching service would pull today.
        .body(StoryRuns.stepPath("pins.dependencies") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.images") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.workspaces") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("pins.projects") + ".status", equalTo("SUCCEEDED"))
        // The registry's own opening measurement needs no pin either, so the night still has the
        // one figure that would show the store growing.
        .body(StoryRuns.stepPath("artifacts.usage.before") + ".status", equalTo("SUCCEEDED"))
        // No keep-set to lose: the two that hang off the disk measurement alone.
        .body(StoryRuns.stepPath("containers.volumes") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("containers.build-cache") + ".status", equalTo("SUCCEEDED"))
        .body(
            StoryRuns.stepPath("containers.build-cache") + ".summary",
            containsString("host 12.6 GB reclaimed"))
        // A different pin pattern one store further out, and unaffected by this one's failure.
        .body(StoryRuns.stepPath("repos.catalogue") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("branches.sweep") + ".status", equalTo("SUCCEEDED"))
        // configuration.entries depends on pins.deployments alone, which answered — so qits-ci's
        // 503 leaves it untouched too, the same "only what needs a pin waits for one" shape.
        .body(StoryRuns.stepPath("configuration.entries") + ".status", equalTo("SUCCEEDED"))
        // …and the service-client sweep waits on the deployer's claims alone, which no pin touches.
        .body(StoryRuns.stepPath("claims.idp-clients") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("idp.service-clients") + ".status", equalTo("SUCCEEDED"));
    story
        .note(
            "everything that needed no pin ran anyway: 12.6 GB of build cache reclaimed, orphan"
                + " volumes swept, host images collected against the deployment pins that DID"
                + " answer, and merged branches swept over a catalogue this failure never touched")
        .as("independent-steps-still-run");

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
            "independent-steps-still-run",
            "failure-skip-cascades")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, FAIL_CLOSED_SLUG, step);
    }

    from(StoryIdentities.OPERATOR, "POST " + StoryTarget.GC_RUNS_PATH + " -> 202");
    from(StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");

    // The broken peer, drawn with the status it answered — evidence rather than a claim.
    to(StoryPeers.CI, StoryPeers.label("GET", StoryPeers.DAEMON_PATH, StoryPeers.REFUSED_STATUS));
    // …and the fifteen calls that happened anyway. Each store was measured once rather than twice,
    // because both after-steps were skipped — it is the same label either way, so the count is what
    // says so and the step assertions above are what make it readable.
    to(StoryPeers.CONTAINERS, StoryPeers.read(StoryPeers.USAGE_PATH));
    to(StoryPeers.ARTIFACTS, StoryPeers.read(StoryPeers.STORE_PATH));
    to(StoryPeers.MAINTENANCE, StoryPeers.read(StoryPeers.DEPENDENCY_PINS_PATH));
    to(StoryPeers.CONFIGURATION, StoryPeers.read(StoryPeers.IMAGE_PINS_PATH));
    to(StoryPeers.WORKSPACES, StoryPeers.read(StoryPeers.WORKSPACE_LAUNCH_PINS_PATH));
    to(StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.PROJECT_LAUNCH_PINS_PATH));
    to(StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.IMAGES_PATH));
    to(StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.VOLUMES_PATH));
    to(StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.BUILD_CACHE_PATH));
    to(StoryPeers.DEPLOYMENTS, StoryPeers.read(StoryPeers.PINS_PATH));
    to(StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.REPOSITORIES_PATH));
    to(StoryPeers.WORKSPACES, StoryPeers.written(StoryPeers.BRANCHES_PATH));
    // configuration.entries only waits on pins.deployments, which answered — qits-ci's 503 does
    // not touch it.
    to(StoryPeers.CONFIGURATION, StoryPeers.written(StoryPeers.CONFIGURATION_ENTRIES_PATH));
    // The service-client sweep waits on the claims read alone, which answered.
    to(StoryPeers.DEPLOYMENTS, StoryPeers.read(StoryPeers.CLAIMS_PATH));
    to(StoryPeers.IDP, StoryPeers.written(StoryPeers.SERVICE_CLIENTS_PATH));

    // THE CLAIM A PRESENCE CHECK CANNOT MAKE. Nothing that DELETES reached the registry — not the
    // plan, not the sweep — because a pin that protects it could not be read. The registry is
    // reached once, for its size, by a step that needs no pin at all, so the absence is stated over
    // the calls rather than over the peer: `assertNoEdgesTo` would now be a claim this story cannot
    // honestly make, and a narrower absence is worth more than a wider one that is false.
    UserflowReport report = ReportAssertions.read(CATEGORY_SLUG, FAIL_CLOSED_SLUG);
    assertTrue(
        report.network().stream()
            .noneMatch(
                edge ->
                    StoryPeers.ARTIFACTS.equals(edge.to()) && edge.label().contains("/gc/")),
        () -> "a collection call reached the registry without its pins: " + report.network());
    // Two in, sixteen out. The credential was minted an hour ago by the first run of the
    // catalogue, so no token arrow belongs here — see StoryPeers on why exactly one story owns that
    // edge.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, FAIL_CLOSED_SLUG, 18);
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
