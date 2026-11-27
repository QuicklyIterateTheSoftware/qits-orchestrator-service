package eu.wohlben.qits.orchestrator.contracts;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasItem;

import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * <b>A seeded, fixed-instant run stays on {@code listProcessRuns}' first page</b> (ticket qits-2733)
 * even once the kind has more runs than the page holds.
 *
 * <p>{@code GET /processes/{kind}/runs} ({@code ProcessController.listProcessRuns}) answers only
 * the newest {@code DEFAULT_LIMIT} (20) runs of a kind, newest first ({@code RunStore.runs}). {@link
 * ProviderStates#aFinishedGcDryRun}, {@link ProviderStates#aFinishedScanRun} and {@link
 * ProviderStates#aFinishedSbomCheckRun} each seed one run at a fixed, long-past instant; without
 * {@link ProviderStates#forgetRuns} clearing the kind's other runs first, twenty newer ones of the
 * same kind push the seeded run off that page, and a recording or a consumer pact reading the
 * listing sees it missing (or, as happened, an empty list).
 *
 * <p>Same application and database as {@link GoldenMasterRecordingTest} and {@link
 * ConsumerPactVerificationTest} ({@link ContractProfile}), the same way they obtain {@link
 * ProviderStates} and apply a state, and the same operator identity they call the API as.
 */
@QuarkusTest
@TestProfile(ContractProfile.class)
@TestSecurity(user = "operator", roles = "qits:admin")
class SeededRunListingTest {

  /**
   * More than {@code ProcessController.DEFAULT_LIMIT} (20; package-private to {@code
   * eu.wohlben.qits.orchestrator.api}, so named again here) — enough newer runs of a kind to push a
   * fixed-past-instant seeded run off the listing's first page if nothing forgets them first.
   */
  private static final int RUNS_TO_OPEN = 21;

  @Inject ProviderStates states;

  @Inject RunStore runs;

  @Test
  void aFinishedGcDryRunStaysListedBehindTwentyNewerGcRuns() {
    assertSeededRunIsListed(GcProcess.KIND, ProviderStates.A_FINISHED_GC_DRY_RUN);
  }

  @Test
  void aFinishedScanRunStaysListedBehindTwentyNewerScanRuns() {
    assertSeededRunIsListed(ScanProcess.KIND, ProviderStates.A_FINISHED_SCAN_RUN);
  }

  @Test
  void aFinishedSbomCheckRunStaysListedBehindTwentyNewerSbomCheckRuns() {
    assertSeededRunIsListed(SbomCheckProcess.KIND, ProviderStates.A_FINISHED_SBOM_CHECK_RUN);
  }

  /**
   * Opens {@value #RUNS_TO_OPEN} real runs of {@code kind} started now, applies {@code state} (whose
   * own run is seeded at a fixed, long-past instant) and asserts the state's {@code runId} param is
   * still in {@code GET /processes/{kind}/runs}' answer.
   */
  private void assertSeededRunIsListed(String kind, String state) {
    states.awaitNoActiveRun();
    openAndCloseOverflowRuns(kind);

    Map<String, String> params = states.params(state);
    String runId = params.get("runId");

    given()
        .header("Accept", "application/json")
        .when()
        .get("/orchestrator/api/processes/" + kind + "/runs")
        .then()
        .statusCode(200)
        .body("id", hasItem(runId));
  }

  /** Mirrors how the provider states write a run: {@code open} then {@code close}, SUCCEEDED. */
  private void openAndCloseOverflowRuns(String kind) {
    for (int i = 0; i < RUNS_TO_OPEN; i++) {
      Instant now = Instant.now();
      UUID runId = runs.open(kind, RunTrigger.SCHEDULED, false, List.of(), now);
      runs.close(runId, RunStatus.SUCCEEDED, "overflow run " + i + " for qits-2733", now);
    }
  }
}
