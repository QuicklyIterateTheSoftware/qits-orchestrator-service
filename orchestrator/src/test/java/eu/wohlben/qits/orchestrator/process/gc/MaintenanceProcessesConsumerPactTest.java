package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * <b>The consumer half of the scan and SBOM check processes' contract with qits-maintenance</b>
 * (ticket qits-2490), the way {@link GcConsumerPactTest} holds the gc rows: for each live row of
 * {@link MaintenanceProcessesContract}, the real step with a real {@code PeerClient} against a
 * pact-jvm mock server answering only what the row {@code consumes} — the mock fails the row unless
 * the step sends the recorded request — and the line the step makes from the cut answer must be
 * the line it makes from the whole one. A row that is not live is reported skipped with what it
 * needs. The committed pact file itself is {@link GcConsumerPactTest}'s, which writes every row of
 * the qits-maintenance pact.
 */
class MaintenanceProcessesConsumerPactTest {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  @TestFactory
  Stream<DynamicTest> everyRowIsWhatTheStepAsksAndReads() {
    return MaintenanceProcessesContract.ROWS.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.kind() + " " + row.stepId() + " -> qits-maintenance-service "
                        + row.interaction().operationId(),
                    () -> verify(row)));
  }

  private static void verify(MaintenanceProcessesContract.Row row) {
    if (!MaintenanceProcessesContract.live(row)) {
      Assumptions.abort(MaintenanceProcessesContract.needs(row));
    }
    StepResult whole = MaintenanceProcessesContract.againstRecording(row);
    assertEquals(RunStatus.SUCCEEDED, whole.status(), String.valueOf(whole.error()));
    assertEquals(
        whole.summary(),
        MaintenanceProcessesContract.againstConsumed(row).summary(),
        "consumes misses a field the step reads");
    GcContract.pact(PeerTarget.MAINTENANCE)
        .run(
            row.interaction(),
            (url, recorded) -> {
              StepResult actual =
                  MaintenanceProcessesContract.run(row, ContractPeers.against(url));
              assertEquals(RunStatus.SUCCEEDED, actual.status(), String.valueOf(actual.error()));
              assertNotNull(actual.summary(), "the step reads the pact's answer");
            });
  }

  /**
   * Each row of {@link MaintenanceProcessesContract#PENDING}, reported skipped with the state it is
   * waiting for — the same shape {@link GcConsumerPactTest} reports {@link GcContract#PENDING}
   * with. Not a failure: the state is not on the classpath yet because qits-maintenance has not
   * published it (qits-2490), and a row here is a promise to switch it on once it does.
   */
  @TestFactory
  Stream<DynamicTest> everyPendingRowWaitsForItsProviderState() {
    return MaintenanceProcessesContract.PENDING.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.stepId() + " -> qits-maintenance-service " + row.operationId()
                        + " [" + row.state() + "]",
                    () -> Assumptions.abort(row.reason())));
  }
}
