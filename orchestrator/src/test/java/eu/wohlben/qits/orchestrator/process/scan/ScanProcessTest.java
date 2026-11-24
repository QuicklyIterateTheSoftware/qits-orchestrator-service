package eu.wohlben.qits.orchestrator.process.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers.Scripted;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The scan process (ticket qits-2490), outside CDI against {@link FakePeers}: the four steps and
 * their edges, what the requests send, and every way a follow ends — SUCCEEDED, FAILED with the
 * scan's message, and still running after the timeout. The wait between reads is a fake that moves
 * a fake clock, so the shipped ten seconds and the hour are never waited out.
 */
class ScanProcessTest {

  private static final String SCAN_ID = "00000000-0000-4000-8000-0000000000a1";

  private static final String SCAN_PATH = ScanProcess.SCANS_PATH + "/" + SCAN_ID;

  /** A clock the sleeper moves forward by exactly what it was asked to wait. */
  private static final class MovingClock extends Clock {
    Instant now = Instant.parse("2026-11-24T00:30:00Z");

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  private MovingClock clock;

  private final List<Duration> waits = new ArrayList<>();

  private FakePeers peers;

  private ScanProcess process;

  @BeforeEach
  void setUp() {
    clock = new MovingClock();
    waits.clear();
    peers = ContractPeers.fake();
    process =
        ScanProcess.of(
            ScanConfig.of(true, Duration.ofSeconds(10), Duration.ofSeconds(30)),
            duration -> {
              waits.add(duration);
              clock.now = clock.now.plus(duration);
            },
            clock);
  }

  private static String scan(String status, String message) {
    return "{\"id\":\""
        + SCAN_ID
        + "\",\"scope\":\"INTERNAL\",\"repository\":null,\"trigger\":\"MANUAL\",\"status\":\""
        + status
        + "\",\"startedAt\":\"2026-11-24T00:30:00Z\",\"finishedAt\":null,\"message\":"
        + (message == null ? "null" : "\"" + message + "\"")
        + "}";
  }

  private StepDefinition step(String id) {
    return process.steps().stream().filter(s -> s.id().equals(id)).findFirst().orElseThrow();
  }

  /** A context whose request step already answered the scan id, as the executor records it. */
  private RunContext requested() {
    RunContext context = new RunContext(UUID.randomUUID(), false, peers);
    String answer = "{\"id\":\"" + SCAN_ID + "\"}";
    try {
      context.record(
          ScanProcess.INTERNAL_REQUEST,
          new com.fasterxml.jackson.databind.ObjectMapper().readTree(answer),
          answer);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return context;
  }

  @Test
  void theFourStepsAreTwoIndependentPairsOnQitsMaintenanceAndNoneIsDryRunnable() {
    assertEquals("scan", process.kind());
    assertEquals("Manifest scan", process.name());
    assertFalse(process.dryRunnable(), "the scan door has no dry run");
    assertFalse(process.agentStartable(), "the agent decision was made for gc alone");
    List<StepDefinition> steps = process.steps();
    assertEquals(
        List.of("internal.request", "internal.follow", "external.request", "external.follow"),
        steps.stream().map(StepDefinition::id).toList());
    assertEquals(List.of(), steps.get(0).dependsOn());
    assertEquals(List.of("internal.request"), steps.get(1).dependsOn());
    assertEquals(List.of(), steps.get(2).dependsOn(), "external does not wait on internal");
    assertEquals(List.of("external.request"), steps.get(3).dependsOn());
    steps.forEach(s -> assertEquals(PeerTarget.MAINTENANCE, s.target(), s.id()));
  }

  @Test
  void eachRequestQueuesTheWholeCatalogueInItsScope() {
    peers.answer(ScanProcess.SCANS_PATH, Scripted.status(202, "{\"id\":\"" + SCAN_ID + "\"}"));
    RunContext context = new RunContext(UUID.randomUUID(), false, peers);
    StepResult internal = step(ScanProcess.INTERNAL_REQUEST).body().run(context);
    StepResult external = step(ScanProcess.EXTERNAL_REQUEST).body().run(context);
    assertEquals(RunStatus.SUCCEEDED, internal.status(), internal.error());
    assertEquals("queued scan " + SCAN_ID, internal.summary());
    assertEquals(RunStatus.SUCCEEDED, external.status(), external.error());
    assertEquals(
        List.of("{\"scope\":\"INTERNAL\"}", "{\"scope\":\"EXTERNAL\"}"),
        peers.bodiesFor(ScanProcess.SCANS_PATH));
    assertEquals("http://maintenance/maintenance/api/scans", internal.call().url());
    assertEquals("POST", internal.call().method());
  }

  @Test
  void aRequestAnsweredWithoutAnIdFails() {
    peers.answer(ScanProcess.SCANS_PATH, Scripted.status(202, "{}"));
    StepResult result =
        step(ScanProcess.INTERNAL_REQUEST)
            .body()
            .run(new RunContext(UUID.randomUUID(), false, peers));
    assertEquals(RunStatus.FAILED, result.status());
    assertTrue(result.error().endsWith("answered 202 with no scan id"), result.error());
  }

  @Test
  void aRefusedRequestFailsWithTheStatus() {
    peers.answer(ScanProcess.SCANS_PATH, Scripted.status(403, "{\"message\":\"no\"}"));
    StepResult result =
        step(ScanProcess.INTERNAL_REQUEST)
            .body()
            .run(new RunContext(UUID.randomUUID(), false, peers));
    assertEquals(RunStatus.FAILED, result.status());
    assertTrue(result.error().endsWith("answered 403"), result.error());
  }

  @Test
  void aFollowReadsUntilTheScanSucceededWaitingThePollIntervalBetweenReads() {
    peers.answerInSequence(
        SCAN_PATH,
        Scripted.ok(scan("REQUESTED", null)),
        Scripted.ok(scan("RUNNING", null)),
        Scripted.ok(scan("SUCCEEDED", null)));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.SUCCEEDED, result.status(), result.error());
    assertEquals("INTERNAL scan SUCCEEDED", result.summary());
    assertEquals(3, peers.callsTo(SCAN_PATH));
    assertEquals(List.of(Duration.ofSeconds(10), Duration.ofSeconds(10)), waits);
    assertTrue(result.answer().body().contains("SUCCEEDED"), "the row keeps the last read");
    assertEquals("GET", result.call().method());
  }

  @Test
  void aScanThatFailedFailsTheStepWithItsOwnMessage() {
    peers.answerInSequence(
        SCAN_PATH,
        Scripted.ok(scan("RUNNING", null)),
        Scripted.ok(scan("FAILED", "the git host answered 502")));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.FAILED, result.status());
    assertEquals("scan " + SCAN_ID + " FAILED: the git host answered 502", result.error());
    assertNull(result.summary());
    assertEquals(2, peers.callsTo(SCAN_PATH));
  }

  @Test
  void aScanThatFailedWithoutAMessageSaysSo() {
    peers.answer(SCAN_PATH, Scripted.ok(scan("FAILED", null)));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.FAILED, result.status());
    assertEquals("scan " + SCAN_ID + " FAILED: qits-maintenance gave no message", result.error());
  }

  @Test
  void aScanStillRunningAfterTheFollowTimeoutFailsTheStepAndIsLeftAlone() {
    peers.answer(SCAN_PATH, Scripted.ok(scan("RUNNING", null)));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.FAILED, result.status());
    assertEquals(
        "scan "
            + SCAN_ID
            + " is still RUNNING after PT30S (qits.orchestrator.scan.follow-timeout); it is left"
            + " to qits-maintenance",
        result.error());
    // Reads at 0, 10, 20 and 30 seconds; the fourth is at the deadline and ends the wait.
    assertEquals(4, peers.callsTo(SCAN_PATH));
    assertEquals(3, waits.size());
    assertTrue(
        peers.calls.stream().allMatch(call -> call.method().equals("GET")),
        "following never writes");
  }

  @Test
  void aReadThatFailsEndsTheFollowAtOnce() {
    peers.answer(SCAN_PATH, Scripted.status(404, "{\"message\":\"no such scan\"}"));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.FAILED, result.status());
    assertTrue(result.error().endsWith("answered 404"), result.error());
    assertEquals(1, peers.callsTo(SCAN_PATH));
    assertEquals(List.of(), waits);
  }

  @Test
  void anUnknownScanStatusFailsTheStep() {
    peers.answer(SCAN_PATH, Scripted.ok(scan("PAUSED", null)));
    StepResult result = step(ScanProcess.INTERNAL_FOLLOW).body().run(requested());
    assertEquals(RunStatus.FAILED, result.status());
    assertTrue(result.error().endsWith("answered a scan whose status is 'PAUSED'"), result.error());
  }

  @Test
  void aFollowWithNoQueuedScanMakesNoCall() {
    StepResult result =
        step(ScanProcess.EXTERNAL_FOLLOW)
            .body()
            .run(new RunContext(UUID.randomUUID(), false, peers));
    assertEquals(RunStatus.FAILED, result.status());
    assertEquals("external.request handed over no scan id to follow", result.error());
    assertEquals(List.of(), peers.calls);
  }

  @Test
  void anInterruptedWaitEndsTheFollowAndKeepsTheFlag() {
    peers.answer(SCAN_PATH, Scripted.ok(scan("RUNNING", null)));
    ScanProcess interrupted =
        ScanProcess.of(
            ScanConfig.of(true, Duration.ofSeconds(10), Duration.ofHours(1)),
            duration -> {
              throw new InterruptedException("shutting down");
            },
            clock);
    try {
      StepResult result =
          interrupted.steps().get(1).body().run(requested());
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is restored");
      assertEquals(RunStatus.FAILED, result.status());
      assertEquals(
          "following scan " + SCAN_ID + " was interrupted while it was RUNNING", result.error());
    } finally {
      Thread.interrupted();
    }
  }
}
