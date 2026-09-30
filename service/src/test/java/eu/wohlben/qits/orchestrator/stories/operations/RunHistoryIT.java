package eu.wohlben.qits.orchestrator.stories.operations;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import eu.wohlben.qits.orchestrator.stories.collection.GarbageCollectionRunIT;
import eu.wohlben.qits.orchestrator.stories.faults.PeerFailureIT;
import eu.wohlben.qits.orchestrator.stories.support.StoryIdentities;
import eu.wohlben.qits.orchestrator.stories.support.StoryNetwork;
import eu.wohlben.qits.orchestrator.stories.support.StoryPeers;
import eu.wohlben.qits.orchestrator.stories.support.StoryProfile;
import eu.wohlben.qits.orchestrator.stories.support.StoryRuns;
import eu.wohlben.qits.orchestrator.stories.support.StoryTarget;
import eu.wohlben.qits.userflows.Interactions;
import eu.wohlben.qits.userflows.Network;
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
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * <b>The two things an operator reads</b>: the plan a run will follow, and the account of one that
 * already ran.
 *
 * <p>They are the same surface from opposite sides of a night, and they cost this service two
 * completely different things — which is the point of putting them beside each other.
 *
 * <ul>
 *   <li><b>The plan</b> is a fact about this artifact and nothing else. {@code GET /processes} reads
 *       the {@code ProcessRegistry}, which is CDI discovery: no store is opened, no peer is called,
 *       and the diagram of that read has exactly one arrow with nothing leaving this process at all.
 *       It is also the pinned contract — {@code qits-orchestrator-plan.md} fixes the step ids and
 *       the edges between them and four repositories build against them — so a fast-jar that lost
 *       the process bean would answer 200 with an empty list and only this assertion would notice.
 *   <li><b>The account</b> is rows, and it is the ONLY account of a deletion that exists on this
 *       platform. Not a cache: nothing else on qits keeps a record of what was deleted, so the
 *       {@code jdbc} edge below is drawn as a declared one — no tap can see a store a process talks
 *       to directly — and it is the only edge in this catalogue that is a claim rather than
 *       evidence.
 * </ul>
 *
 * <p><b>These stories read what the run stories wrote</b>, so they run after them. That is stated
 * with {@code @UserflowRunsAfter} as well as being true of the package names, and it is a real
 * dependency rather than tidiness: a history of a service nothing has run on would be an empty page
 * asserting nothing. Run this class on its own and it fails loudly, which is the right way for that
 * assumption to break.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RunHistoryIT {

  static final String CATEGORY = "operations";

  static final String CATEGORY_SLUG = Slugs.slug(CATEGORY);

  static final String PLAN = "An operator reads the plan a run will follow";

  static final String ACCOUNT = "An operator reads the account of a run that already happened";

  static final String PLAN_SLUG = Slugs.slug(PLAN);

  static final String ACCOUNT_SLUG = Slugs.slug(ACCOUNT);

  /** How the diagram names this service's own database — the store a declared edge points at. */
  static final String STORE = "postgresql";

  /** The declared edge's label. An authored literal: {@code Labels} would not rewrite a word of it. */
  static final String STORE_LABEL = "the run log — op_run and op_step, the only account of a deletion";

  @BeforeAll
  static void tapBothEndsOfTheNetwork() {
    StoryNetwork.install();
  }

  @UserStory(value = PLAN, category = CATEGORY)
  @UserStoryDescription(
      """
      Before pressing a button that starts deletions on six other services, an operator reads what
      the button does. The process catalogue is that answer: every technical process this service
      knows, each with its steps in the order they run, the peer each one calls, and the
      dependencies between them — which is what decides what a failure skips.

      It is deliberately the cheapest read this service has. It names nothing, it opens no store and
      it calls no peer: the registry is CDI discovery, so the answer is a fact about this artifact
      rather than about anybody else's state. That is why the client can draw the graph before a run
      exists, and why a page showing the plan cannot be made slow by a peer having a bad night.

      What it serves is also the pinned contract. The step ids and the edges between them are fixed
      in the platform's plan document and four repositories build against them, so this listing is
      the plan itself rather than a description of it.
      """)
  @UserflowRunsAfter(GarbageCollectionRunIT.class)
  @Order(1)
  void thePlanIsAFactAboutThisArtifactAndCostsNothingToRead(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.PROCESSES_PATH)
        .then()
        .statusCode(200)
        .body("kind", hasItem(StoryTarget.GC))
        .body(gc() + ".name", equalTo("Garbage collection"))
        .body(gc() + ".description", notNullValue())
        .body(gc() + ".steps.size()", equalTo(18))
        .body(gc() + ".steps.id[0]", equalTo("usage.before"))
        .body(gc() + ".steps.id", hasItem("branches.sweep"))
        .body(gc() + ".steps.target", hasItem("workspaces"));
    story
        .note(
            "the catalogue: one process, eighteen steps, each naming the peer it calls — the graph"
                + " the client draws before a run exists")
        .as("plan-served");

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.PROCESSES_PATH)
        .then()
        .statusCode(200)
        // The fail-closed edge, and it is asserted BECAUSE it is a promise made on the wire.
        .body(
            step("artifacts.plan") + ".dependsOn",
            contains(
                "pins.deployments",
                "pins.ci",
                "pins.dependencies",
                "pins.images",
                "pins.workspaces",
                "pins.projects"))
        // …and the one that deliberately is NOT there: a prune has no keep-set, so hanging it off
        // the image sweep would cost the platform its largest reclaim on a broken pin read.
        .body(step("containers.build-cache") + ".dependsOn", contains("usage.before"))
        .body(step("usage.after") + ".dependsOn", hasItem("artifacts.sweep"));
    story
        .note(
            "the edges are the reading: the registry plan waits on all six pin reads, while the"
                + " build cache prune waits on the disk measurement alone — a step with no keep-set must"
                + " not be stopped by a pin it never needed")
        .as("edges-are-the-contract");
  }

  @UserStory(value = ACCOUNT, category = CATEGORY)
  @UserStoryDescription(
      """
      A gc run is unattended by design — the clock starts it at three in the morning — so what an
      operator actually reads is the account afterwards. The history lists every run of a kind,
      newest first, with what started it, whether it was allowed to delete, how it ended and its
      one-line summary. Opening one gives every step: its status, the request that went out, the
      peer's answer whole, and the sentence read out of that answer.

      The request body and the response are STRINGS carrying JSON text rather than embedded objects,
      and that is a contract rather than an oversight: a peer's answer is stored as the bytes that
      arrived, bounded at a megabyte with a truncation marker past it, and a truncated document is
      not parseable JSON so it could not live in an object-typed field. A client parses the string
      and copes with one that does not parse.

      Reading it costs nothing but rows. No peer is called and nothing is recomputed — which is what
      makes this the account rather than a second opinion. It is also the only one that exists:
      nothing else on this platform keeps a record of what a collection deleted, which is why the
      run log is its own database and never a cache.
      """)
  @UserflowRunsAfter({GarbageCollectionRunIT.class, PeerFailureIT.class})
  @Order(2)
  void theAccountOfADeletionIsRowsAndNothingElse(Interactions story, Network network) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    JsonPath history =
        StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
            .when()
            .get(StoryTarget.GC_RUNS_PATH + "?limit=10")
            .then()
            .statusCode(200)
            // Every run this catalogue started was a person or a machine asking for it. The clock
            // is off in every suite here, so `scheduled` is a trigger no story can produce.
            .body("trigger", everyItem(equalTo("manual")))
            .body("status", hasItem("SUCCEEDED"))
            .body("status", hasItem("FAILED"))
            .body("dryRun", hasItem(true))
            .body("dryRun", hasItem(false))
            .extract()
            .jsonPath();
    story
        .note(
            "the history, newest first: four runs — one dry, one that a broken peer failed, and the"
                + " ones that collected — each with what started it and its one-line summary")
        .as("history-listed");

    String id = history.getString("find { it.status == 'SUCCEEDED' && it.dryRun == false }.id");
    assertNotNull(id, "no completed real run to read back: " + history.prettify());

    StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT)
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body("id", equalTo(id))
        .body("kind", equalTo(StoryTarget.GC))
        .body("trigger", equalTo("manual"))
        .body("finishedAt", notNullValue())
        .body("steps.size()", equalTo(18))
        .body(StoryRuns.stepPath("usage.before") + ".target", equalTo("containers"))
        .body(StoryRuns.stepPath("usage.before") + ".request.method", equalTo("GET"))
        .body(
            StoryRuns.stepPath("usage.before") + ".request.url",
            containsString(StoryPeers.USAGE_PATH))
        // The peer's answer as the TEXT it arrived as — a string, not an object.
        .body(StoryRuns.stepPath("usage.before") + ".response", containsString("\"sizeBytes\""))
        .body(StoryRuns.stepPath("branches.sweep") + ".request.body", containsString("keepPrefixes"))
        .body(StoryRuns.stepPath("branches.sweep") + ".response", containsString("branchesExamined"));
    story
        .note(
            "and one run opened: every step with the url it called, the body it sent, the answer it"
                + " got back whole — stored as the bytes that arrived, which is why request.body and"
                + " response are strings a client parses rather than objects this service re-shaped")
        .as("run-opened");

    // The store, and the one edge in this catalogue that is a CLAIM rather than evidence: a
    // database a process talks to directly is invisible to every tap, so it is declared and the
    // renderers draw it muted and dashed. What it points at is the only account of a deletion this
    // platform has — not a cache, which is why it is this service's own database.
    network.declare(NetworkEdge.JDBC, StoryTarget.SERVICE, STORE, STORE_LABEL);
  }

  @AfterAll
  static void everyOperationsStoryIsComplete() {
    // --- the plan -------------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, PLAN_SLUG, UserflowReport.PASSED);
    ReportAssertions.assertStepId(CATEGORY_SLUG, PLAN_SLUG, "plan-served");
    ReportAssertions.assertStepId(CATEGORY_SLUG, PLAN_SLUG, "edges-are-the-contract");
    from(PLAN_SLUG, "GET " + StoryTarget.PROCESSES_PATH + " -> 200");
    // ONE arrow, and the absence beside it is the whole claim: two reads of the plan and NOTHING
    // left this process to answer either of them. No store, no peer — the registry is CDI
    // discovery, so a page drawing the graph cannot be made slow by anybody else's bad night.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, PLAN_SLUG, 1);
    ReportAssertions.assertNoEdgesFrom(CATEGORY_SLUG, PLAN_SLUG, StoryTarget.SERVICE);

    // --- the account ----------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, ACCOUNT_SLUG, UserflowReport.PASSED);
    ReportAssertions.assertStepId(CATEGORY_SLUG, ACCOUNT_SLUG, "history-listed");
    ReportAssertions.assertStepId(CATEGORY_SLUG, ACCOUNT_SLUG, "run-opened");
    // The query is dropped by the shipped tap, so `?limit=10` is invisible here — which is right:
    // two routes differing only in their query are one dependency.
    from(ACCOUNT_SLUG, "GET " + StoryTarget.GC_RUNS_PATH + " -> 200");
    from(ACCOUNT_SLUG, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    ReportAssertions.assertDeclaredEdge(
        CATEGORY_SLUG, ACCOUNT_SLUG, NetworkEdge.JDBC, StoryTarget.SERVICE, STORE, STORE_LABEL);
    // Two reads and one store. Nothing was recomputed and no peer was asked — reading what a
    // deletion did must never be able to start another one.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, ACCOUNT_SLUG, 3);
    for (String peer : StoryPeers.ALL) {
      ReportAssertions.assertNoEdgesTo(CATEGORY_SLUG, ACCOUNT_SLUG, peer);
    }
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG, ACCOUNT_SLUG, List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));
  }

  private static String gc() {
    return "find { it.kind == '" + StoryTarget.GC + "' }";
  }

  private static String step(String id) {
    return gc() + ".steps.find { it.id == '" + id + "' }";
  }

  private static void from(String slug, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG,
        slug,
        NetworkEdge.HTTP,
        StoryIdentities.OPERATOR,
        StoryTarget.SERVICE,
        label);
  }
}
