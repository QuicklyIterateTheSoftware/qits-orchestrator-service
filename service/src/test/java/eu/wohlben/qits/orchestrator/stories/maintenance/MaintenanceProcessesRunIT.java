package eu.wohlben.qits.orchestrator.stories.maintenance;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * <b>qits-maintenance's nightly chores, run from here</b> (ticket qits-2490): the manifest scans and
 * the SBOM check, which were crons inside qits-maintenance with a log line for an account, as two
 * processes with a run history, a Run now and events.
 *
 * <p>Two stories: a person runs the scans now — both queued and followed to their end, and a dry
 * run refused because the door has none — and a person runs the SBOM check now, whose one call is
 * synchronous. Both run after the gc stories, whose first run owns the outbound token mint, so
 * neither draws it.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MaintenanceProcessesRunIT {

  static final String CATEGORY = "maintenance";

  static final String CATEGORY_SLUG = Slugs.slug(CATEGORY);

  static final String SCANNED = "An operator runs the night's manifest scans now";

  static final String CHECKED = "An operator runs the SBOM check now";

  static final String SCANNED_SLUG = Slugs.slug(SCANNED);

  static final String CHECKED_SLUG = Slugs.slug(CHECKED);

  static final String SCAN_RUNS_PATH = StoryTarget.PROCESSES_PATH + "/scan/runs";

  static final String SBOM_CHECK_RUNS_PATH = StoryTarget.PROCESSES_PATH + "/sbom-check/runs";

  @BeforeAll
  static void tapBothEndsOfTheNetwork() {
    StoryNetwork.install();
  }

  private static Supplier<RequestSpecification> operator() {
    return () -> StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT);
  }

  /** Starts a real run of one kind and hands back its id. */
  private static String start(String runsPath) {
    return operator()
        .get()
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(runsPath)
        .then()
        .statusCode(202)
        .contentType(ContentType.JSON)
        .extract()
        .path("id");
  }

  @UserStory(value = SCANNED, category = CATEGORY)
  @UserStoryDescription(
      """
      qits-maintenance knows which version of every dependency each repository pins and which is
      the latest, and it learns both from a scan of every repository's manifests. Two scans a
      night: the platform's own artifacts (INTERNAL) and everything published elsewhere (EXTERNAL).
      They used to be two clocks inside qits-maintenance, and a night they failed was a log line.

      Now they are a process here. An operator presses Run now; one run queues the internal scan
      and follows it to its end, then queues the external one and follows that. A scan that fails
      fails its step with qits-maintenance's own message, and one still running after an hour fails
      it saying so — the scan itself is left alone. The external pair does not depend on the
      internal one: a broken internal scan says nothing about the registries the external one reads.

      There is no dry run. The door has no such mode, so a "dry" scan would be a real one wearing
      the wrong label; the button is hidden and the request is refused.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  @Order(1)
  void anOperatorRunsTheScansAndEachIsFollowedToItsEnd(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    operator()
        .get()
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(true))
        .when()
        .post(SCAN_RUNS_PATH)
        .then()
        .statusCode(400)
        .body("message", containsString("the scan process has no dry run"));
    story
        .note(
            "a dry run is refused with a 400 before anything is written: the scan door has no dry"
                + " mode, so there is no run that changes nothing to start")
        .as("dry-run-refused");

    String id = start(SCAN_RUNS_PATH);
    story
        .note("the operator presses Run now; the answer is 202 and an id, as for any process")
        .as("scan-run-accepted");

    JsonPath run = StoryRuns.detail(operator(), id);
    assertEquals("SUCCEEDED", run.getString("status"), "the run did not succeed: " + run.prettify());
    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body("kind", equalTo("scan"))
        .body("dryRun", equalTo(false))
        .body("steps.id", equalTo(List.of(
            "internal.request", "internal.follow", "external.request", "external.follow")))
        .body("steps.status", everyItem(equalTo("SUCCEEDED")))
        .body(StoryRuns.stepPath("internal.request") + ".request.body",
            equalTo("{\"scope\":\"INTERNAL\"}"))
        .body(StoryRuns.stepPath("external.request") + ".request.body",
            equalTo("{\"scope\":\"EXTERNAL\"}"))
        .body(StoryRuns.stepPath("internal.follow") + ".summary",
            equalTo("INTERNAL scan SUCCEEDED"))
        .body(StoryRuns.stepPath("internal.follow") + ".response",
            containsString(StoryPeers.SCAN_ID));
    story
        .note(
            "four steps: each scan queued for the whole catalogue in its scope, then read until it"
                + " ended — the row keeps the last read, so the scan's own account is one click away")
        .as("scans-followed");
  }

  @UserStory(value = CHECKED, category = CATEGORY)
  @UserStoryDescription(
      """
      Every released version still in the registry should carry a usable SBOM. qits-maintenance
      checks that once a day: it re-reads the missing and failed ones from qits-artifacts and keeps
      one ticket open per artifact that still has none. It was a clock inside qits-maintenance; now
      it is a process here with a run history and a Run now.

      The run is one call, and it is synchronous: qits-maintenance answers with the report it stored
      once the whole check is done, so the call has a read timeout of its own. The step's line is
      read off that report — the versions without an SBOM, the open tickets, the warnings — and
      computes nothing.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  @Order(2)
  void anOperatorRunsTheSbomCheckAndReadsItsReport(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    String id = start(SBOM_CHECK_RUNS_PATH);
    story.note("the operator presses Run now; 202 and an id").as("check-run-accepted");

    JsonPath run = StoryRuns.detail(operator(), id);
    assertEquals("SUCCEEDED", run.getString("status"), "the run did not succeed: " + run.prettify());
    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body("kind", equalTo("sbom-check"))
        .body("steps.size()", equalTo(1))
        .body(StoryRuns.stepPath("check") + ".request.method", equalTo("POST"))
        .body(
            StoryRuns.stepPath("check") + ".summary",
            equalTo("1 version without a usable SBOM, 1 ticket open, 0 warnings"));
    story
        .note(
            "one step: the check, answered with its report, and the line read off it — one version"
                + " without an SBOM, one ticket holding it")
        .as("report-read");
  }

  @AfterAll
  static void everyMaintenanceStoryIsComplete() {
    // --- the scans -----------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, SCANNED_SLUG, UserflowReport.PASSED);
    for (String step : List.of("dry-run-refused", "scan-run-accepted", "scans-followed")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, SCANNED_SLUG, step);
    }
    from(SCANNED_SLUG, "POST " + SCAN_RUNS_PATH + " -> 400");
    from(SCANNED_SLUG, "POST " + SCAN_RUNS_PATH + " -> 202");
    from(SCANNED_SLUG, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    // Two scans are two POSTs and two reads of one path each: one arrow per path. The scan id is
    // a uuid, so the follow's read is templated like a run id.
    to(SCANNED_SLUG, StoryPeers.written(StoryPeers.SCANS_PATH));
    to(SCANNED_SLUG, StoryPeers.read(StoryPeers.SCAN_PATH));
    // FIVE: three doors and two calls. The refused dry run drew an arrow to this service and none
    // beyond it.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, SCANNED_SLUG, 5);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG, SCANNED_SLUG, List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));

    // --- the SBOM check ------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, CHECKED_SLUG, UserflowReport.PASSED);
    for (String step : List.of("check-run-accepted", "report-read")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, CHECKED_SLUG, step);
    }
    from(CHECKED_SLUG, "POST " + SBOM_CHECK_RUNS_PATH + " -> 202");
    from(CHECKED_SLUG, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    to(CHECKED_SLUG, StoryPeers.written(StoryPeers.SBOM_CHECK_PATH));
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, CHECKED_SLUG, 3);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG, CHECKED_SLUG, List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));

    for (String slug : List.of(SCANNED_SLUG, CHECKED_SLUG)) {
      ReportAssertions.assertNotLeaked(CATEGORY_SLUG, slug, StoryProfile.CLIENT_SECRET);
    }
  }

  private static void from(String slug, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, slug, NetworkEdge.HTTP, StoryIdentities.OPERATOR, StoryTarget.SERVICE, label);
  }

  private static void to(String slug, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, slug, NetworkEdge.HTTP, StoryTarget.SERVICE, StoryPeers.MAINTENANCE, label);
  }
}
