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
 * on some other answer cannot hide behind this pass. A {@link GcContract#PENDING} row's call counts
 * as a row: it is written down, waiting only on its provider's state, and it names its call itself
 * because there is no recording to read it from.
 *
 * <p><b>Excluded, by name</b>:
 *
 * <ul>
 *   <li>the {@code DeploymentActive} catch-up against qits-events ({@code GET /events/api/events}),
 *       which {@code service/}'s listener makes through qits-eventstream-javalib. That call is the
 *       library's, so its pact is owned and verified as qits-eventstream-javalib's pact ({@code
 *       qits-eventstream-javalib_qits-events-service.json}, which qits-events-service pins and
 *       verifies); this service commits no events pact of its own.
 *   <li>the service's own health probes, which call no peer.
 * </ul>
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
    for (GcContract.Pending pending : GcContract.PENDING) {
      rows.add(pending.target() + " " + pending.method() + " " + pending.path());
    }
    assertEquals(rows, made, "the calls a gc pass makes against the rows of GcContract");
  }

  /**
   * The one exception to "one call per step" (qits-1733): a pin read whose peer answers 503 is asked
   * again — the same call, so it adds no row — with the configured wait between asks, and nothing
   * but a 503 earns a second ask.
   */
  @Test
  void aPinReadIsAskedAgainOnlyOnA503AndWaitsTheConfiguredDelayBetweenAsks() {
    String path = "/maintenance/api/pins";
    GcConfig config = GcContract.shippedConfig();
    assertEquals(6, config.pinsRetryAttempts(), "the shipped attempt count");
    assertEquals(java.time.Duration.ofSeconds(30), config.pinsRetryDelay(), "the shipped delay");
    GcProcess process = GcContract.process(config);
    List<java.time.Duration> waits = new ArrayList<>();
    process.sleeper = waits::add;
    StepDefinition step = GcContract.step(process, GcProcess.PINS_DEPENDENCIES);

    FakePeers peers = ContractPeers.fake();
    peers.answer(path, FakePeers.Scripted.status(503, "{\"message\":\"not complete yet\"}"));
    StepResult result = step.body().run(new RunContext(UUID.randomUUID(), false, peers));
    assertEquals(6, peers.callsTo(path));
    assertEquals(java.util.Collections.nCopies(5, java.time.Duration.ofSeconds(30)), waits);
    assertEquals(eu.wohlben.qits.orchestrator.run.RunStatus.FAILED, result.status());
    assertTrue(
        result.error().endsWith("/maintenance/api/pins answered 503 (6 attempts, 30s apart)"),
        result.error());

    peers.reset();
    waits.clear();
    peers.answer(path, FakePeers.Scripted.status(502, "{}"));
    result = step.body().run(new RunContext(UUID.randomUUID(), false, peers));
    assertEquals(1, peers.callsTo(path), "a 502 is not asked again");
    assertEquals(List.of(), waits);
    assertTrue(result.error().endsWith("answered 502"), result.error());
  }

  /** An interrupt stops the asking: the flag survives and the last read is the verdict. */
  @Test
  void anInterruptedWaitStopsAskingAndKeepsTheFlag() {
    String path = "/maintenance/api/pins";
    GcProcess process = GcContract.process(GcContract.shippedConfig());
    process.sleeper =
        duration -> {
          throw new InterruptedException("shutting down");
        };
    FakePeers peers = ContractPeers.fake();
    peers.answer(path, FakePeers.Scripted.status(503, "{}"));
    try {
      StepResult result =
          GcContract.step(process, GcProcess.PINS_DEPENDENCIES)
              .body()
              .run(new RunContext(UUID.randomUUID(), false, peers));
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is restored");
      assertEquals(1, peers.callsTo(path));
      assertEquals(eu.wohlben.qits.orchestrator.run.RunStatus.FAILED, result.status());
      assertTrue(result.error().endsWith("answered 503"), result.error());
    } finally {
      Thread.interrupted();
    }
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
    for (GcContract.Pending pending : GcContract.PENDING) {
      StepDefinition step = GcContract.step(process, pending.stepId());
      assertEquals(
          step.target(), pending.target(), pending.stepId() + ": the pending row names the step's peer");
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
