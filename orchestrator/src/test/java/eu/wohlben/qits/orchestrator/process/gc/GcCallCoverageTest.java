package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerCall;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.peer.PeerTokens;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.pact.consumer.GoldenMasters.Operation;
import io.quarkus.oidc.client.OidcClient;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * <b>The guard</b> (ticket qits-1149): every call the gc process makes to a peer has a row in
 * {@link GcContract}, and every row is a call the process still makes. The set of calls is not
 * written down here; it is what a full, non-dry gc pass sends, captured by a {@link FakePeers}
 * whose every peer has a url of its own. A new step, or a step that starts calling another path,
 * fails this test until it has a row.
 *
 * <p>The pass answers each call with the provider's recording where a row names one, and with no
 * answer otherwise; every step must have made exactly one call, so a step that would only call
 * on some other answer cannot hide behind this pass.
 *
 * <p><b>Not peer calls, and so not here</b>: the {@code DeploymentActive} catch-up against
 * qits-events, which {@code service/}'s listener makes through qits-eventstream-javalib, and the
 * service's own health probes.
 */
class GcCallCoverageTest {

  @Test
  void everyCallOfAFullGcPassHasARowAndEveryRowIsACall() {
    FakePeers peers = ContractPeers.fake();
    for (GcContract.Row row : GcContract.ROWS) {
      peers.answer(
          GcContract.recorded(row).examplePath(),
          FakePeers.Scripted.ok(
              GcContract.masters(row)
                  .body(row.interaction().state(), row.interaction().operationId())));
    }
    GcProcess process = GcContract.process(GcContract.shippedConfig());
    RunContext context = new RunContext(UUID.randomUUID(), false, peers);
    List<String> silent = new ArrayList<>();
    for (StepDefinition step : process.steps()) {
      int before = peers.calls.size();
      StepResult result = step.body().run(context);
      if (peers.calls.size() != before + 1) {
        silent.add(step.id() + " made " + (peers.calls.size() - before) + " calls");
      }
      if (result.answer() != null && result.answer().json() != null) {
        context.record(step.id(), result.answer().json(), result.answer().body());
      }
    }
    assertEquals(List.of(), silent, "every gc step makes exactly one peer call in a full pass");

    Set<String> made = new TreeSet<>();
    for (PeerCall call : peers.calls) {
      made.add(ContractPeers.target(call) + " " + call.method() + " " + ContractPeers.path(call));
    }
    Set<String> rows = new TreeSet<>();
    for (GcContract.Row row : GcContract.ROWS) {
      Operation op = GcContract.recorded(row);
      rows.add(row.target() + " " + op.method() + " " + op.examplePath());
    }
    assertEquals(rows, made, "the calls a gc pass makes against the rows of GcContract");
  }

  @Test
  void everyRowIsAStepAndEveryPeerIsCalled() {
    GcProcess process = GcContract.process(GcContract.shippedConfig());
    Set<String> steps =
        process.steps().stream().map(StepDefinition::id).collect(Collectors.toCollection(TreeSet::new));
    Set<String> rowSteps =
        GcContract.ROWS.stream().map(GcContract.Row::stepId).collect(Collectors.toCollection(TreeSet::new));
    assertTrue(steps.containsAll(rowSteps), "every row names a gc step: " + rowSteps);

    Set<String> targets = new TreeSet<>();
    for (Field field : PeerTarget.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          targets.add((String) field.get(null));
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    Set<String> stepTargets =
        process.steps().stream().map(StepDefinition::target).collect(Collectors.toCollection(TreeSet::new));
    assertEquals(targets, stepTargets, "every PeerTarget is a peer some gc step calls");
    assertEquals(targets, new TreeSet<>(GcContract.PEERS.keySet()), "every PeerTarget has a pact");
    for (GcContract.Row row : GcContract.ROWS) {
      StepDefinition step = GcContract.step(process, row.stepId());
      assertEquals(step.target(), row.target(), row.stepId() + ": the row names the step's peer");
    }
  }

  @Test
  void theTokenMintHasARowWhilePeerTokensMintsThroughAnOidcClient() {
    boolean mints =
        Arrays.stream(PeerTokens.class.getDeclaredFields())
            .anyMatch(field -> OidcClient.class.isAssignableFrom(field.getType()));
    assertTrue(mints, "PeerTokens no longer holds an OidcClient: drop GcContract.TOKEN_MINT");
    assertTrue(
        GcContract.pact(PeerTarget.IDP).rows().contains(GcContract.TOKEN_MINT),
        "the qits-idp pact holds the token mint");
  }
}
