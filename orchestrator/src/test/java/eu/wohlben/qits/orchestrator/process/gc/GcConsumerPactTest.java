package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.pact.consumer.ConsumerPact;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * <b>The consumer half of the gc process's contracts</b> (ticket qits-1149): for each row of
 * {@link GcContract}, the real step, with a real {@link eu.wohlben.qits.orchestrator.peer.PeerClient}
 * making a real HTTP call, against a pact-jvm mock server answering the row's interaction — only
 * the fields the row {@code consumes}. The mock server fails the row unless the step sends the
 * provider's recorded request.
 *
 * <p>The step must SUCCEED against the mock. Separately, the line it makes from the provider's
 * recorded answer cut to {@code consumes} must be the line it makes from the whole answer: a field
 * the summary reads but {@code consumes} leaves out shows up as a different line. (The mock's own
 * answer cannot be compared that way: an array matched by type answers its first element's values
 * for every element, so a sum over it differs from the recording's.)
 *
 * <p>Then, per peer, the committed {@code pacts/qits-orchestrator-service_<provider>.json} is what
 * the rows write ({@code -Dgolden.update=true} rewrites it), and every interaction carries both
 * references.
 */
class GcConsumerPactTest {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  /** Every {@link PeerTarget}, in the order the pacts are reported. */
  static final List<String> TARGETS =
      List.of(
          PeerTarget.CONTAINERS,
          PeerTarget.ARTIFACTS,
          PeerTarget.DEPLOYMENTS,
          PeerTarget.CI,
          PeerTarget.MAINTENANCE,
          PeerTarget.WORKSPACES,
          PeerTarget.PROJECTS,
          PeerTarget.CONFIGURATION,
          PeerTarget.IDP);

  @TestFactory
  Stream<DynamicTest> everyRowIsWhatTheStepAsksAndReads() {
    return GcContract.ROWS.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.stepId() + " -> " + GcContract.masters(row).repository() + " "
                        + row.interaction().operationId(),
                    () -> verify(row)));
  }

  private static void verify(GcContract.Row row) {
    StepResult whole = GcContract.againstRecording(row);
    assertEquals(RunStatus.SUCCEEDED, whole.status(), String.valueOf(whole.error()));
    assertEquals(
        whole.summary(),
        GcContract.againstConsumed(row).summary(),
        "consumes misses a field the step reads");
    GcContract.pact(row.target())
        .run(
            row.interaction(),
            (url, recorded) -> {
              StepResult actual = GcContract.run(row, ContractPeers.against(url));
              assertEquals(RunStatus.SUCCEEDED, actual.status(), String.valueOf(actual.error()));
              assertNotNull(actual.summary(), "the step reads the pact's answer");
            });
  }

  @TestFactory
  Stream<DynamicTest> theCommittedPactsAreWhatTheRowsWrite() {
    return TARGETS.stream()
        .map(
            target -> {
              ConsumerPact pact = GcContract.pact(target);
              return DynamicTest.dynamicTest(
                  pact.file(),
                  () -> {
                    pact.compareOrWritePactFile();
                    pact.assertEveryInteractionCarriesBothReferences();
                  });
            });
  }
}
