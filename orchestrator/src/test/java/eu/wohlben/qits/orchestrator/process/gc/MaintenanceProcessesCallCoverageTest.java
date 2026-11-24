package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerCall;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * <b>The guard</b> (ticket qits-2490), {@link GcCallCoverageTest}'s for the scan and SBOM check
 * processes: every call a full pass of each makes has a row in {@link
 * MaintenanceProcessesContract}, and every row is a call one of them still makes. The pass answers
 * each call with the recording where there is one; {@code requestScan}, which is recorded for one
 * repository rather than the whole catalogue, is answered with the recorded scan id so the follow
 * reads the recorded scan. A row that is not live counts all the same — it is written down, waiting
 * only on its recording.
 */
class MaintenanceProcessesCallCoverageTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void everyCallOfAFullPassHasARowAndEveryRowIsACall() throws Exception {
    FakePeers peers = ContractPeers.fake();
    MaintenanceProcessesContract.recordedAnswers()
        .forEach((path, body) -> peers.answer(path, FakePeers.Scripted.ok(body)));
    String scanId =
        MaintenanceProcessesContract.MAINTENANCE
            .params(MaintenanceProcessesContract.A_FINISHED_SCAN)
            .get("scanId");
    peers.answer(
        ScanProcess.SCANS_PATH, FakePeers.Scripted.status(202, "{\"id\":\"" + scanId + "\"}"));

    List<String> wrong = new ArrayList<>();
    for (TechnicalProcess process :
        List.of(MaintenanceProcessesContract.scan(), MaintenanceProcessesContract.sbomCheck())) {
      RunContext context = new RunContext(UUID.randomUUID(), false, peers);
      for (StepDefinition step : process.steps()) {
        int before = peers.calls.size();
        StepResult result = step.body().run(context);
        if (peers.calls.size() != before + 1) {
          wrong.add(step.id() + " made " + (peers.calls.size() - before) + " calls");
        }
        if (result.status() != RunStatus.SUCCEEDED) {
          wrong.add(step.id() + " " + result.status() + ": " + result.error());
        }
        if (result.answer() != null && result.answer().json() != null) {
          context.record(step.id(), result.answer().json(), result.answer().body());
        }
      }
    }
    assertEquals(List.of(), wrong, "every step makes exactly one call against a recording");

    Set<String> made = new TreeSet<>();
    for (PeerCall call : peers.calls) {
      made.add(ContractPeers.target(call) + " " + call.method() + " " + ContractPeers.path(call));
    }
    Set<String> rows = new TreeSet<>();
    for (MaintenanceProcessesContract.Row row : MaintenanceProcessesContract.ROWS) {
      rows.add(MaintenanceProcessesContract.call(row));
    }
    assertEquals(rows, made, "the calls a full pass makes against the rows");

    // The request bodies the rows say the steps send, are what they send.
    List<String> sent = peers.bodiesFor(ScanProcess.SCANS_PATH);
    assertEquals(List.of("{\"scope\":\"INTERNAL\"}", "{\"scope\":\"EXTERNAL\"}"), sent);
    for (MaintenanceProcessesContract.Row row : MaintenanceProcessesContract.ROWS) {
      if (row.body() != null) {
        assertTrue(sent.contains(JSON.readTree(row.body()).toString()), row.stepId());
      }
    }
    assertEquals(
        java.util.Collections.singletonList(null),
        peers.bodiesFor(SbomCheckProcess.RUNS_PATH),
        "the SBOM check sends no body");
  }

  @Test
  void everyRowIsAStepOfItsProcessOnQitsMaintenance() {
    for (MaintenanceProcessesContract.Row row : MaintenanceProcessesContract.ROWS) {
      StepDefinition step =
          MaintenanceProcessesContract.step(
              MaintenanceProcessesContract.process(row.kind()), row.stepId());
      assertEquals(PeerTarget.MAINTENANCE, step.target(), row.stepId());
    }
  }
}
