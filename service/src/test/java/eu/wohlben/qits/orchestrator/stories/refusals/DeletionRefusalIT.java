package eu.wohlben.qits.orchestrator.stories.refusals;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

import eu.wohlben.qits.orchestrator.stories.support.StoryIdentities;
import eu.wohlben.qits.orchestrator.stories.support.StoryNetwork;
import eu.wohlben.qits.orchestrator.stories.support.StoryPeers;
import eu.wohlben.qits.orchestrator.stories.support.StoryProfile;
import eu.wohlben.qits.orchestrator.stories.support.StoryTarget;
import eu.wohlben.qits.userflows.Interactions;
import eu.wohlben.qits.userflows.NetworkCapture;
import eu.wohlben.qits.userflows.NetworkEdge;
import eu.wohlben.qits.userflows.UserStory;
import eu.wohlben.qits.userflows.UserStoryDescription;
import eu.wohlben.qits.userflows.report.ReportAssertions;
import eu.wohlben.qits.userflows.report.Slugs;
import eu.wohlben.qits.userflows.report.UserflowReport;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * <b>The doors on the write surface</b> — the one that starts deletions on nine other services.
 *
 * <p>{@code api.TokenValidationBootstrapIT} pins the same refusals on the cheapest READ this
 * service has, because that is what a story about token validation should drive. These two are
 * about the other end: {@code POST /processes/{kind}/runs}, which hands work to a single-threaded
 * worker that then asks qits-artifacts and qits-containers to delete things. There is no anonymous
 * route here and there must never be one, and the reason is on the far side of this door rather
 * than in it.
 *
 * <p><b>What a refusal must cost is nothing.</b> A presence check cannot say that; an absence can.
 * Both stories therefore end with {@code assertNoEdgesTo} for every one of the nine peers — the
 * refusal left an arrow at this service's door and not one beyond it.
 *
 * <p><b>Both role tracks open every route here, so neither is refused the other's door.</b> That is
 * unusual on this platform and it is deliberate: an operator presses Run now in a browser and a
 * machine may post the same run, so a machine-only guard would lock the operator out of the button
 * this service exists to offer. What IS refused is a third role — a real platform role no route
 * here names — which authenticates perfectly and covers nothing. That is the 403 rather than the
 * 401, and the difference is whether the credential ever became an identity.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DeletionRefusalIT {

  static final String CATEGORY = "refusals";

  static final String CATEGORY_SLUG = Slugs.slug(CATEGORY);

  static final String UNAUTHORISED = "Nobody without the platform's own roles starts a deletion";

  static final String UNKNOWN = "A process this platform does not have, and a run that never was";

  static final String UNAUTHORISED_SLUG = Slugs.slug(UNAUTHORISED);

  static final String UNKNOWN_SLUG = Slugs.slug(UNKNOWN);

  private static final List<String> MINTED = new ArrayList<>();

  @BeforeAll
  static void tapBothEndsOfTheNetwork() {
    StoryNetwork.install();
  }

  @UserStory(value = UNAUTHORISED, category = CATEGORY)
  @UserStoryDescription(
      """
      This service's write surface is one route, and behind it are somebody else's disks. Starting a
      run asks qits-artifacts to unlink blobs, qits-containers to remove images and prune the build
      cache, and qits-workspaces to delete refs. So the door takes a credential, always, on both of
      the platform's identity tracks.

      A request carrying nothing at all satisfies neither mechanism and is refused before any of it
      begins. A bearer minted outside this platform is refused the same way, however well-formed it
      looks — both are 401 and not 403, because the credential never became an identity and there is
      no caller to have been forbidden. A sibling service's bearer is not that case: every token
      qits-idp mints carries the one platform audience, so a peer is admitted here and its
      roles decide what it may do.

      A caller holding a role this service has never heard of gets the other answer. qits:reader is
      a real platform role and it is not one of the two these routes name; presented as a session's
      forwarded header or minted into a token addressed here, it authenticates perfectly and covers
      nothing. 403 is what "you may not" looks like when "who are you" was already answered.

      And every refusal costs the platform nothing: no run row, no worker, and not one request to
      any of the nine services a real run would have driven.
      """)
  @Order(1)
  void aRefusedCallerStartsNothingAtAll(Interactions story) {
    NetworkCapture.actor(StoryIdentities.ANONYMOUS);
    given()
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(401);
    story
        .note(
            "a request carrying no credential at all satisfies neither the bearer track nor the"
                + " edge's forwarded pair, and is refused before anything begins")
        .as("anonymous-refused");

    NetworkCapture.actor(StoryIdentities.IMPOSTOR);
    String offPlatform = StoryIdentities.offPlatformToken("story-impostor");
    MINTED.add(offPlatform);
    StoryIdentities.bearer(given(), offPlatform)
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(401);
    story
        .note(
            "a bearer addressed to an audience this platform never mints is refused just the same:"
                + " the audience says a token belongs to this platform, and nothing else does")
        .as("wrong-audience-refused");

    // One actor and two credentials, which the diagram draws as ONE arrow: same initiator, same
    // route, same status. That is the right division — the graph says who reached what and got
    // what, the steps say why.
    NetworkCapture.actor(StoryIdentities.WRONG_ROLE);
    StoryIdentities.person(given(), "story-reader", StoryIdentities.UNPRIVILEGED_ROLE)
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(403);
    story
        .note(
            "a logged-in session holding qits:reader is a real caller this service knows and will"
                + " not let near a deletion — 403, not 401, because it authenticated")
        .as("wrong-role-session-refused");

    String unprivileged = StoryIdentities.unprivilegedToken("story-reader-machine");
    MINTED.add(unprivileged);
    StoryIdentities.bearer(given(), unprivileged)
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(403);
    story
        .note(
            "and the same role in a bearer addressed here gets the same answer, which is what says"
                + " the groups→roles mapping really ran rather than being waved through")
        .as("wrong-role-bearer-refused");
  }

  @UserStory(value = UNKNOWN, category = CATEGORY)
  @UserStoryDescription(
      """
      An authenticated operator can still ask for something that is not there, and what this surface
      answers then is a decision rather than a default.

      An unknown process kind is a 404 and never an empty list: "this process has never run" and
      "there is no such process" are different answers, and a client that could not tell them apart
      would show an empty page for a typo. Both routes that take a kind refuse it the same way.

      A run id that no row carries is a 404 too — and so is one that is not a uuid at all. The route
      takes the id as a STRING and turns a malformed one into the same refusal, because a malformed
      id and an absent one are the same question from the caller's side and one mistake should not
      produce two refusals.

      Every one of them is answered out of this process alone. Nothing is asked of any peer to find
      out that something does not exist.
      """)
  @Order(2)
  void whatIsNotThereIsAlwaysA404AndNeverAnEmptyAnswer(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(true))
        .when()
        .post(StoryTarget.PROCESSES_PATH + "/" + StoryTarget.UNKNOWN_KIND + "/runs")
        .then()
        .statusCode(404)
        .body("message", containsString("no technical process"));
    story
        .note("starting a process this platform does not have is a 404 naming the kind")
        .as("unknown-kind-start-refused");

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.PROCESSES_PATH + "/" + StoryTarget.UNKNOWN_KIND + "/runs")
        .then()
        .statusCode(404);
    story
        .note(
            "and so is asking for its history — an empty list would say `this has never run`, which"
                + " is a different and wrong answer to a typo")
        .as("unknown-kind-history-refused");

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.runPath(UUID.randomUUID().toString()))
        .then()
        .statusCode(404)
        .body("message", containsString("no run"));
    story.note("a run id no row carries is a 404").as("unknown-run-refused");

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.runPath(StoryTarget.MALFORMED_RUN_ID))
        .then()
        .statusCode(404);
    story
        .note(
            "and an id that is not a uuid at all is the same 404: from the caller's side a"
                + " malformed id and an absent one are one question, so they get one refusal")
        .as("malformed-run-id-refused");
  }

  @AfterAll
  static void everyRefusalStoryIsComplete() {
    // --- the write surface -----------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, UNAUTHORISED_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of(
            "anonymous-refused",
            "wrong-audience-refused",
            "wrong-role-session-refused",
            "wrong-role-bearer-refused")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, UNAUTHORISED_SLUG, step);
    }
    String start = "POST " + StoryTarget.GC_RUNS_PATH;
    edge(UNAUTHORISED_SLUG, StoryIdentities.ANONYMOUS, start + " -> 401");
    edge(UNAUTHORISED_SLUG, StoryIdentities.IMPOSTOR, start + " -> 401");
    // Four requests, three arrows: the session and the bearer holding qits:reader are one initiator
    // getting one answer over one route, so the diagram draws them once and the notes keep them
    // apart.
    edge(UNAUTHORISED_SLUG, StoryIdentities.WRONG_ROLE, start + " -> 403");
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, UNAUTHORISED_SLUG, 3);
    ReportAssertions.assertNoEdgesFrom(CATEGORY_SLUG, UNAUTHORISED_SLUG, StoryTarget.SERVICE);
    for (String peer : StoryPeers.ALL) {
      // THE POINT OF THE STORY. Four attempts to start a deletion and not one request left this
      // process — no run row, no worker, nobody's disk touched.
      ReportAssertions.assertNoEdgesTo(CATEGORY_SLUG, UNAUTHORISED_SLUG, peer);
    }

    // --- what is not there -----------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, UNKNOWN_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of(
            "unknown-kind-start-refused",
            "unknown-kind-history-refused",
            "unknown-run-refused",
            "malformed-run-id-refused")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, UNKNOWN_SLUG, step);
    }
    String unknownRuns = StoryTarget.PROCESSES_PATH + "/" + StoryTarget.UNKNOWN_KIND + "/runs";
    edge(UNKNOWN_SLUG, StoryIdentities.OPERATOR, "POST " + unknownRuns + " -> 404");
    edge(UNKNOWN_SLUG, StoryIdentities.OPERATOR, "GET " + unknownRuns + " -> 404");
    // The generated uuid is rewritten to {id} and the authored `not-a-uuid` is not, which is the
    // whole of the scrubber's rule: one is a value this run made up, the other is a value the story
    // typed. Two different labels, two arrows, and the diagram says the second refusal happened.
    edge(UNKNOWN_SLUG, StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 404");
    edge(
        UNKNOWN_SLUG,
        StoryIdentities.OPERATOR,
        "GET " + StoryTarget.RUNS_PATH + "/" + StoryTarget.MALFORMED_RUN_ID + " -> 404");
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, UNKNOWN_SLUG, 4);
    ReportAssertions.assertNoEdgesFrom(CATEGORY_SLUG, UNKNOWN_SLUG, StoryTarget.SERVICE);

    for (String slug : List.of(UNAUTHORISED_SLUG, UNKNOWN_SLUG)) {
      for (String bearer : MINTED) {
        ReportAssertions.assertNotLeaked(CATEGORY_SLUG, slug, bearer);
      }
    }
  }

  private static void edge(String slug, String actor, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, slug, NetworkEdge.HTTP, actor, StoryTarget.SERVICE, label);
  }
}
