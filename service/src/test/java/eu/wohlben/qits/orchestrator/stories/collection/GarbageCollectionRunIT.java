package eu.wohlben.qits.orchestrator.stories.collection;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.api.TokenValidationBootstrapIT;
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * <b>The gc run</b> — the one technical process this platform has, driven end to end against eight
 * peers that answer.
 *
 * <p>This is the catalogue's centre, because it is the only place the whole design is visible at
 * once. A process only SENDS REQUESTS: it deletes nothing, holds no docker socket, opens no store
 * but its own run log, and makes no decision an owner has not published as an API. Everything it
 * does therefore happens on the far side of a socket, and a diagram of what it did is the only
 * complete account of a run there is. {@link StoryPeers} is that far side — one stub answering as
 * qits-containers, qits-artifacts, qits-ci, qits-platform-deployments, qits-projects,
 * qits-workspaces, qits-platform-maintenance and qits-configuration, told apart by path prefix,
 * plus qits-platform-idp for the credential this service presents to each of them.
 *
 * <p><b>Three stories, three runs, and each one is a different sentence about the same nineteen
 * steps:</b>
 *
 * <ol>
 *   <li>a real run — every peer called, every answer summarised, the pin set read once and handed
 *       on;
 *   <li>a dry run — the same run with the one step that deletes withheld, and everything behind it
 *       still running, which is a distinction this service learned the hard way;
 *   <li>a second run refused while the first is going, because two runs would be two plans against
 *       two moments of the same stores.
 * </ol>
 *
 * <p><b>This class owns the outbound credential arrow.</b> quarkus-oidc-client caches its mint, so
 * {@code POST /idp/token} happens on the first peer call of the whole catalogue and never again —
 * see {@link StoryPeers}. It lands in story 1 and nowhere else, which is why story 2 and story 3
 * have one fewer edge for a reason that has nothing to do with what they do.
 *
 * <p><b>It also owns the run history everything after it reads.</b> {@code stories.operations} and
 * {@code stories.faults} come later by package name; {@code @UserflowRunsAfter} states the
 * dependency on the boot story, which is the one that has to drain the startup JWKS fetch first.
 */
@QuarkusIntegrationTest
@TestProfile(StoryProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GarbageCollectionRunIT {

  static final String CATEGORY = "garbage collection";

  static final String CATEGORY_SLUG = Slugs.slug(CATEGORY);

  static final String COLLECTED = "The platform collects what nothing pins";

  static final String MEASURED = "A dry run measures the platform without deleting anything";

  static final String SERIALISED = "Only one deletion run at a time";

  static final String COLLECTED_SLUG = Slugs.slug(COLLECTED);

  static final String MEASURED_SLUG = Slugs.slug(MEASURED);

  static final String SERIALISED_SLUG = Slugs.slug(SERIALISED);

  /** Every credential a story here minted, so the reports can be searched for all of them. */
  private static final List<String> MINTED = new ArrayList<>();

  @BeforeAll
  static void tapBothEndsOfTheNetwork() {
    StoryNetwork.install();
  }

  /** The person pressing Run now: the forward-auth pair the platform edge asserts for a session. */
  private static Supplier<RequestSpecification> operator() {
    return () -> StoryIdentities.person(given(), StoryIdentities.OPERATOR_ACCOUNT);
  }

  @UserStory(value = COLLECTED, category = CATEGORY)
  @UserStoryDescription(
      """
      The platform's disk grows every night: images every build pushes, blobs behind every tag,
      dangling volumes, buildkit cache. One run reclaims all of it, and the whole of what this
      service contributes is ORDER and a PIN SET — it deletes nothing itself.

      An operator presses Run now. The answer is a 202 and an id, because the work is minutes of
      somebody else's pruning; the browser then polls the run while it happens. Nineteen steps run
      in declaration order: the disk and the registry store are measured, the six pin reads say what
      must survive, the registry is planned and swept, host images, orphan volumes and build cache
      are collected, the repository catalogue is read and merged branches are swept, retired
      configuration entries are removed, decommissioned git tags are swept on the host and its
      backup twin, and both stores are measured again.

      The pins are the point. A pin is something a collection must not delete — an image sha a
      restart or a rollback would pull, the ci daemon binary a run would launch, a version a
      repository's main still references, a container image the next deploy would configure, the
      image a launch would pull today — and they are read ONCE and handed to every deleter, so every
      deleter judges against one answer taken at one moment. They are read here rather than by
      qits-artifacts because this is the one component that holds an identity for every peer; the
      readers that used to live over there had no credential and were being refused.

      Four of the six are CONSUMPTION rather than deployment, and nothing else on the platform can
      answer for them: no deployment names the library a pom pins, and none names the workspace
      image a person's next click pulls. Before they were read, the only thing keeping either alive
      was how recently somebody happened to ask for it.

      The last two are the same claim in a different TENSE, and it is the tense that matters.
      qits-configuration says which version the NEXT deploy of a launching service would be given;
      qits-workspaces and qits-projects each say what a start would pull TODAY, out of the config
      they are actually running with. A service keeps launching the version it was deployed with
      until it is redeployed, so the two answers disagree for as long as that gap lasts — and the
      image nobody had pinned was always the one everybody was pulling.

      What each step reports is READ out of the peer's own answer and never derived. A summary that
      recomputed "what would die" would be a second policy, and two policies in one report is
      exactly the mistake the whole design refuses.
      """)
  @UserflowRunsAfter(TokenValidationBootstrapIT.class)
  @Order(1)
  void anOperatorStartsARunAndEveryOwnerDeletesItsOwnStore(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    String id = StoryRuns.start(operator(), false);
    story
        .note(
            "the operator presses Run now; the answer is 202 and an id, because a gc run is minutes"
                + " of somebody else's pruning and an HTTP request is the wrong place to hold it")
        .as("run-accepted");

    JsonPath run = StoryRuns.detail(operator(), id);
    assertEquals("SUCCEEDED", run.getString("status"), "the run did not succeed: " + run.prettify());
    story
        .note(
            "the browser polls the run while it happens; nineteen steps later it is SUCCEEDED and"
                + " the run's own summary is one line per step")
        .as("run-succeeded");

    // The nineteen steps, each with the line it read out of its peer's answer. Asserting the SENTENCE
    // rather than a status is what pins "computes nothing": every figure below is a number the
    // owner of that store reported, quoted back.
    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body("steps.size()", equalTo(19))
        .body("steps.status", everyItem(equalTo("SUCCEEDED")))
        .body(
            StoryRuns.stepPath("usage.before") + ".summary",
            equalTo("images 43.5 GB (19.3 GB reclaimable), build cache 35.1 GB"))
        .body(
            StoryRuns.stepPath("artifacts.usage.before") + ".summary",
            equalTo("store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)"))
        .body(StoryRuns.stepPath("pins.deployments") + ".summary", equalTo("1 application pinned"))
        .body(
            StoryRuns.stepPath("pins.ci") + ".summary",
            equalTo("qits-ci-daemon 2026.815.120000 (previous 2026.814.101010)"))
        .body(
            StoryRuns.stepPath("pins.dependencies") + ".summary",
            equalTo("1 manifest pin across 1 repository"))
        .body(
            StoryRuns.stepPath("pins.images") + ".summary",
            equalTo("1 configured container image"))
        .body(
            StoryRuns.stepPath("pins.workspaces") + ".summary",
            equalTo("1 launch image — what a workspace/editor start would pull today"))
        .body(
            StoryRuns.stepPath("pins.projects") + ".summary",
            equalTo("1 launch image — what an agent/refinement start would pull today"))
        .body(
            StoryRuns.stepPath("artifacts.plan") + ".summary",
            equalTo("128 identities, 19.3 GB reclaimable, executable=true"))
        .body(
            StoryRuns.stepPath("artifacts.sweep") + ".summary",
            equalTo("91 blobs unlinked, 17.8 GB reclaimed"))
        .body(
            StoryRuns.stepPath("containers.images") + ".summary",
            equalTo("2 images removed, 9.4 GB reclaimed, 2 kept"))
        .body(
            StoryRuns.stepPath("containers.volumes") + ".summary",
            equalTo("1 volumes removed, 1 kept"))
        .body(
            StoryRuns.stepPath("containers.build-cache") + ".summary",
            equalTo("host 12.6 GB reclaimed, 1 builder 3.1 GB reclaimed"))
        .body(
            StoryRuns.stepPath("repos.catalogue") + ".summary",
            equalTo("1 repository in the catalogue"))
        .body(
            StoryRuns.stepPath("branches.sweep") + ".summary",
            equalTo("removed 1 of 214 branches across 1 repositories"))
        .body(
            StoryRuns.stepPath("configuration.entries") + ".summary",
            equalTo(
                "removed 1 of 1 entries; kept: unpinned 0, undeclaredPinnedVersion 0, pinned 1,"
                    + " inFlight 0, neverDeclared 0, staged 0; 0 errors"))
        .body(
            StoryRuns.stepPath("tags.sweep") + ".summary",
            equalTo(
                "1 tags decommissioned across 1 repositories (1 host, 1 twin); kept: newest 0,"
                    + " pinnedVersion 1, gitlink 0, inFlight 0, young 0; 0 errors"))
        .body(
            StoryRuns.stepPath("artifacts.usage.after") + ".summary",
            equalTo("store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)"))
        .body(
            StoryRuns.stepPath("usage.after") + ".summary",
            equalTo("images 43.5 GB (19.3 GB reclaimable), build cache 35.1 GB"));
    story
        .note(
            "every step's one line is READ out of that peer's own answer — 19.3 GB reclaimable, 128"
                + " identities condemned, 91 blobs unlinked — and this service computes none of it")
        .as("summaries-are-quotations");

    // The pins, and the claim that makes them worth reading here rather than at each deleter: the
    // registry plan carries BOTH answers as the bytes they arrived as. It is a claim about a body,
    // and a body is a step rather than an edge — the arrow to qits-artifacts is already on the
    // diagram, and what is new is what travelled along it.
    String planRequest =
        run.getString(StoryRuns.stepPath("artifacts.plan") + ".request.body");
    assertTrue(
        planRequest.contains("\"deployments\"")
            && planRequest.contains("\"ciDaemon\"")
            && planRequest.contains("\"dependencies\"")
            && planRequest.contains("\"configuredImages\"")
            && planRequest.contains("\"workspaceLaunches\"")
            && planRequest.contains("\"projectLaunches\""),
        "the registry plan did not carry all six pin sources: " + planRequest);
    assertTrue(
        planRequest.contains(StoryPeers.PINNED_SHA)
            && planRequest.contains(StoryPeers.CI_DAEMON_VERSION)
            && planRequest.contains(StoryPeers.MANIFEST_PIN_VERSION)
            && planRequest.contains(StoryPeers.CONFIGURED_IMAGE_VERSION)
            && planRequest.contains(StoryPeers.LAUNCHED_IMAGE_VERSION),
        "the pins were re-shaped rather than handed on verbatim: " + planRequest);
    story
        .note(
            "all six pin answers travel into the registry plan VERBATIM — the deployer's document,"
                + " the ci daemon's, the manifest dependencies every repository's main references,"
                + " the configured container images and what each launching service would pull"
                + " today — re-embedded rather than re-shaped, because every one of those contracts"
                + " belongs to another repository")
        .as("pins-handed-on-verbatim");

    // The one place a pin answer IS re-shaped, and it is a translation rather than a projection: a
    // deployment pins (applicationName, shas); the tag the host carries for one is
    // qits/<applicationName>:<sha>, which is what ci pushes and what the deployer pulls.
    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body(
            StoryRuns.stepPath("containers.images") + ".request.body",
            containsString(StoryPeers.PINNED_IMAGE))
        .body(
            StoryRuns.stepPath("containers.images") + ".request.body",
            containsString("qits/build-images/"))
        .body(
            StoryRuns.stepPath("containers.images") + ".request.body",
            containsString("\"minAge\":\"PT6H\""))
        .body(
            StoryRuns.stepPath("containers.build-cache") + ".request.body",
            containsString("\"builderKeepStorageBytes\":1073741824"));
    story
        .note(
            "the image keep-set is that pin answer translated into the local tags the host carries,"
                + " plus the configured prefixes no rule may condemn — and the build cache carries"
                + " TWO budgets, because a bootstrap builder is not the cache every build re-reads")
        .as("keep-set-is-the-pin-set");
  }

  @UserStory(value = MEASURED, category = CATEGORY)
  @UserStoryDescription(
      """
      A platform that wants to watch the figures before letting anything delete runs the same
      process with one flag. A dry run still calls every peer — each one plans instead of deleting —
      so its numbers are real numbers rather than a rehearsal.

      Exactly one step is withheld: the registry sweep, because qits-artifacts' sweep has no dry
      mode. Everything else carries the flag into its own request body and the owner judges by it.
      The merged-branch sweep is the clearest case: qits-workspaces judges a dry run identically and
      deletes nothing, so the request goes out rather than being withheld and the dry figures are
      the real ones.

      And the step behind the withheld one still runs. That distinction was measured, not designed:
      the platform's first real dry run answered 200 to all nine calls and still reported the disk
      measurement as "skipped: artifacts.sweep failed", because the executor read every
      non-SUCCEEDED dependency as broken. A green run that reads as broken is worse than a red one,
      so the reason for a skip became a field — a process choosing not to call is a SATISFIED
      dependency, and only a real failure cascades.
      """)
  @UserflowRunsAfter(TokenValidationBootstrapIT.class)
  @Order(2)
  void aDryRunWithholdsTheOneStepThatDeletesAndNothingElse(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);

    String id = StoryRuns.start(operator(), true);
    JsonPath run = StoryRuns.detail(operator(), id);
    assertEquals("SUCCEEDED", run.getString("status"), "the dry run did not succeed: " + run.prettify());
    story
        .note("the operator asks for a dry run: the same nineteen steps, with dryRun on the request")
        .as("dry-run-accepted");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body("dryRun", equalTo(true))
        // The one withheld step. `error` carries the reason and there is no request at all, which
        // is what "made no call" means on the wire.
        .body(StoryRuns.stepPath("artifacts.sweep") + ".status", equalTo("SKIPPED"))
        .body(StoryRuns.stepPath("artifacts.sweep") + ".error", equalTo("dry run"))
        .body(StoryRuns.stepPath("artifacts.sweep") + ".request", nullValue())
        // …and the step that DEPENDS on it, which ran anyway. This is the whole claim.
        .body(StoryRuns.stepPath("usage.after") + ".status", equalTo("SUCCEEDED"))
        .body(StoryRuns.stepPath("usage.after") + ".dependsOn", hasItem("artifacts.sweep"));
    story
        .note(
            "the registry sweep is SKIPPED with `dry run` as its reason and no request beside it —"
                + " and the disk measurement that DEPENDS on it still runs, because a step the"
                + " process chose not to make is a satisfied dependency and not a failure")
        .as("policy-skip-does-not-cascade");

    operator()
        .get()
        .when()
        .get(StoryTarget.runPath(id))
        .then()
        .statusCode(200)
        .body(
            StoryRuns.stepPath("branches.sweep") + ".request.body",
            containsString("\"dryRun\":true"))
        .body(
            StoryRuns.stepPath("branches.sweep") + ".summary",
            containsString("removed 1 of 214 branches across 1 repositories (dry run)"))
        .body(
            StoryRuns.stepPath("containers.images") + ".request.body",
            containsString("\"dryRun\":true"))
        .body(
            StoryRuns.stepPath("containers.volumes") + ".request.body",
            containsString("\"dryRun\":true"));
    story
        .note(
            "every other deleter is still CALLED, with the flag in its body: the branch sweep is"
                + " judged identically by qits-workspaces and deletes nothing, so a dry night's"
                + " figures are a real measurement rather than a rehearsal")
        .as("dry-figures-are-real-figures");
  }

  @UserStory(value = SERIALISED, category = CATEGORY)
  @UserStoryDescription(
      """
      Two gc runs at once would be two plans computed at two moments of the same stores, each handed
      to a deleter the other is also driving. So there is one worker thread for the whole service
      and, in front of it, a refusal: a second run of a kind that is already going is answered 409
      with the id of the run that holds the place.

      A person and the cron arriving together is the ordinary case rather than a race worth losing,
      which is why the check that refuses lives INSIDE the transaction that opens a run rather than
      being a read in front of it. Both callers here are real: the operator who pressed the button,
      and a machine holding this service's own audience — because every route takes qits:admin and
      qits:system alike, and a machine may post the same run a person can.

      The refusal costs nothing and starts nothing: no row, no worker, and not one request to any of
      the six peers. What the diagram shows reaching them is the FIRST run's work, which carries on
      unaffected.
      """)
  @UserflowRunsAfter(TokenValidationBootstrapIT.class)
  @Order(3)
  void aSecondRunIsRefusedWhileTheFirstIsStillGoing(Interactions story) {
    NetworkCapture.actor(StoryIdentities.OPERATOR);
    String id = StoryRuns.start(operator(), false);
    story.note("the operator starts a run; it is RUNNING before the 202 is written").as("first-run-started");

    // A different caller, a different credential and a different arrow: the machine track, which
    // opens exactly the same routes a person's session does.
    NetworkCapture.actor(StoryIdentities.MACHINE);
    String bearer = StoryIdentities.machineToken("story-scheduler");
    MINTED.add(bearer);
    StoryIdentities.bearer(given(), bearer)
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(false))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(409)
        .body("message", containsString("already active"))
        .body("message", containsString(id));
    story
        .note(
            "a machine posts the same run seconds later and is refused 409 — naming the run that"
                + " holds the place, so a caller told `already active` can go and read it")
        .as("second-run-refused");

    NetworkCapture.actor(StoryIdentities.OPERATOR);
    String status = StoryRuns.awaitClosed(operator(), id);
    assertEquals("SUCCEEDED", status, "the first run did not survive the refusal of the second");
    story
        .note(
            "the first run is untouched by the refusal and finishes SUCCEEDED: the second caller"
                + " left no row, no worker and not one request to any peer")
        .as("first-run-unaffected");
  }

  @AfterAll
  static void everyRunStoryIsComplete() {
    // --- the real run --------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, COLLECTED_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of(
            "run-accepted",
            "run-succeeded",
            "summaries-are-quotations",
            "pins-handed-on-verbatim",
            "keep-set-is-the-pin-set")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, COLLECTED_SLUG, step);
    }
    // What the operator sent. The polling loop is many requests and ONE arrow: the run id is a uuid
    // and the label is templated, so what the diagram says is that a run is watched over this
    // route rather than how impatient the watching was.
    from(COLLECTED_SLUG, StoryIdentities.OPERATOR, "POST " + StoryTarget.GC_RUNS_PATH + " -> 202");
    from(COLLECTED_SLUG, StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    // …and what the run sent. Seventeen calls to eight owners, drawn from the far side's own
    // recording, because a process that only sends requests leaves its evidence nowhere else.
    everyPeerCallOf(COLLECTED_SLUG);
    to(COLLECTED_SLUG, StoryPeers.ARTIFACTS, StoryPeers.written(StoryPeers.SWEEP_PATH));
    // The credential this service presents to all eight. Eight clients minted eight tokens on this
    // run and they are ONE arrow — an edge is (kind, from, to, label) and the eight agree in all
    // four — and the mint is cached for an hour, so no later story carries it.
    to(COLLECTED_SLUG, StoryPeers.IDP, StoryPeers.written(StoryPeers.TOKEN_PATH));
    // TWO in, eighteen out. Each store is measured twice and draws once, because the before and the
    // after of a store are deliberately the same call: the run's own measurement of what it
    // achieved, taken by the component that owns the store rather than added up from what each step
    // claimed. There are two such pairs — the host's disk and the registry's bytes — and the second
    // exists because the first cannot see it.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, COLLECTED_SLUG, 20);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG, COLLECTED_SLUG, List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));

    // --- the dry run ---------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, MEASURED_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of("dry-run-accepted", "policy-skip-does-not-cascade", "dry-figures-are-real-figures")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, MEASURED_SLUG, step);
    }
    from(MEASURED_SLUG, StoryIdentities.OPERATOR, "POST " + StoryTarget.GC_RUNS_PATH + " -> 202");
    from(MEASURED_SLUG, StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    everyPeerCallOf(MEASURED_SLUG);
    // EIGHTEEN, one fewer than the real run's nineteen outbound-and-inbound set: the registry sweep
    // is the only arrow a dry run does not draw. qits-artifacts is still reached — the PLAN is what
    // a dry run is for, and so is the store measurement either side of it — so this is a count
    // rather than an absence, and the count is the assertion that would notice a withheld step
    // quietly making its call after all.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, MEASURED_SLUG, 18);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG, MEASURED_SLUG, List.of(StoryIdentities.OPERATOR, StoryTarget.SERVICE));

    // --- one at a time -------------------------------------------------------------------------
    ReportAssertions.assertComplete(CATEGORY_SLUG, SERIALISED_SLUG, UserflowReport.PASSED);
    for (String step :
        List.of("first-run-started", "second-run-refused", "first-run-unaffected")) {
      ReportAssertions.assertStepId(CATEGORY_SLUG, SERIALISED_SLUG, step);
    }
    from(SERIALISED_SLUG, StoryIdentities.OPERATOR, "POST " + StoryTarget.GC_RUNS_PATH + " -> 202");
    from(SERIALISED_SLUG, StoryIdentities.MACHINE, "POST " + StoryTarget.GC_RUNS_PATH + " -> 409");
    from(SERIALISED_SLUG, StoryIdentities.OPERATOR, "GET " + StoryTarget.RUN_LABEL_PATH + " -> 200");
    everyPeerCallOf(SERIALISED_SLUG);
    to(SERIALISED_SLUG, StoryPeers.ARTIFACTS, StoryPeers.written(StoryPeers.SWEEP_PATH));
    // TWENTY: three doors and seventeen peer calls. The refused caller added an arrow to this
    // service and none beyond it — which is the point of the story, and the reason the count is
    // asserted rather than only the 409.
    ReportAssertions.assertEdgeCount(CATEGORY_SLUG, SERIALISED_SLUG, 20);
    ReportAssertions.assertOnlyEdgesFrom(
        CATEGORY_SLUG,
        SERIALISED_SLUG,
        List.of(StoryIdentities.OPERATOR, StoryIdentities.MACHINE, StoryTarget.SERVICE));

    for (String slug : List.of(COLLECTED_SLUG, MEASURED_SLUG, SERIALISED_SLUG)) {
      ReportAssertions.assertNotLeaked(CATEGORY_SLUG, slug, StoryProfile.CLIENT_SECRET);
      for (String bearer : MINTED) {
        ReportAssertions.assertNotLeaked(CATEGORY_SLUG, slug, bearer);
      }
    }
  }

  /**
   * The sixteen calls every gc run makes whatever else is true of it — the sweep is the
   * seventeenth.
   */
  private static void everyPeerCallOf(String slug) {
    to(slug, StoryPeers.CONTAINERS, StoryPeers.read(StoryPeers.USAGE_PATH));
    // The registry's own bytes, read twice and drawn once — the plane the docker measurement cannot
    // see, and the one a night of unnoticed growth was hiding in.
    to(slug, StoryPeers.ARTIFACTS, StoryPeers.read(StoryPeers.STORE_PATH));
    to(slug, StoryPeers.MAINTENANCE, StoryPeers.read(StoryPeers.DEPENDENCY_PINS_PATH));
    to(slug, StoryPeers.CONFIGURATION, StoryPeers.read(StoryPeers.IMAGE_PINS_PATH));
    // The two effective reads, and they draw as arrows to peers this run reaches for other reasons
    // as well — which is the point: a second path on a socket that was already open.
    to(slug, StoryPeers.WORKSPACES, StoryPeers.read(StoryPeers.WORKSPACE_LAUNCH_PINS_PATH));
    to(slug, StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.PROJECT_LAUNCH_PINS_PATH));
    to(slug, StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.IMAGES_PATH));
    to(slug, StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.VOLUMES_PATH));
    to(slug, StoryPeers.CONTAINERS, StoryPeers.written(StoryPeers.BUILD_CACHE_PATH));
    to(slug, StoryPeers.DEPLOYMENTS, StoryPeers.read(StoryPeers.PINS_PATH));
    to(slug, StoryPeers.CI, StoryPeers.read(StoryPeers.DAEMON_PATH));
    to(slug, StoryPeers.ARTIFACTS, StoryPeers.written(StoryPeers.PLAN_PATH));
    to(slug, StoryPeers.PROJECTS, StoryPeers.read(StoryPeers.REPOSITORIES_PATH));
    to(slug, StoryPeers.WORKSPACES, StoryPeers.written(StoryPeers.BRANCHES_PATH));
    to(slug, StoryPeers.CONFIGURATION, StoryPeers.written(StoryPeers.CONFIGURATION_ENTRIES_PATH));
    to(slug, StoryPeers.PROJECTS, StoryPeers.written(StoryPeers.TAGS_SWEEP_PATH));
  }

  private static void from(String slug, String actor, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, slug, NetworkEdge.HTTP, actor, StoryTarget.SERVICE, label);
  }

  private static void to(String slug, String peer, String label) {
    ReportAssertions.assertEdge(
        CATEGORY_SLUG, slug, NetworkEdge.HTTP, StoryTarget.SERVICE, peer, label);
  }
}
