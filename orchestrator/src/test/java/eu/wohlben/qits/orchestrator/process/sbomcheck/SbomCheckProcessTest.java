package eu.wohlben.qits.orchestrator.process.sbomcheck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers.Scripted;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The SBOM check process (ticket qits-2490), outside CDI against {@link FakePeers}: one step, one
 * body-less POST with its own read timeout, and a summary read off the report.
 */
class SbomCheckProcessTest {

  private static final Duration TIMEOUT = Duration.ofMinutes(15);

  private static final String REPORT =
      "{\"ranAt\":\"2026-11-24T02:15:00Z\",\"filed\":true,"
          + "\"entries\":[{\"project\":\"qits\",\"repository\":\"qits-ci-service\","
          + "\"ecosystem\":\"maven\",\"name\":\"eu.wohlben.qits:qits-ci\",\"version\":\"1\","
          + "\"reason\":\"MISSING\"},{\"project\":\"qits\",\"repository\":\"qits-ci-service\","
          + "\"ecosystem\":\"maven\",\"name\":\"eu.wohlben.qits:qits-ci\",\"version\":\"2\","
          + "\"reason\":\"FAILED\"}],"
          + "\"warnings\":[\"one row carried no project\"],"
          + "\"tickets\":[{\"project\":\"qits\",\"ecosystem\":\"maven\","
          + "\"name\":\"eu.wohlben.qits:qits-ci\",\"ticketSlug\":\"qits-703\","
          + "\"versions\":[\"1\",\"2\"]}]}";

  /** A fake that also remembers the read timeout each POST asked for. */
  private static final class TimedPeers extends FakePeers {
    final List<Duration> timeouts = new ArrayList<>();

    @Override
    public PeerExchange post(String target, String path, String body, Duration timeout) {
      timeouts.add(timeout);
      return super.post(target, path, body, timeout);
    }
  }

  private static TimedPeers peers() {
    return ContractPeers.fake(new TimedPeers());
  }

  private static StepResult check(FakePeers peers) {
    SbomCheckProcess process = SbomCheckProcess.of(SbomCheckConfig.of(true, TIMEOUT));
    StepDefinition step = process.steps().getFirst();
    return step.body().run(new RunContext(UUID.randomUUID(), false, peers));
  }

  @Test
  void oneStepOnQitsMaintenanceAndNoDryRun() {
    SbomCheckProcess process = SbomCheckProcess.of(SbomCheckConfig.of(true, TIMEOUT));
    assertEquals("sbom-check", process.kind());
    assertEquals("SBOM check", process.name());
    assertFalse(process.dryRunnable(), "the check door has no dry run");
    assertFalse(process.agentStartable(), "the agent decision was made for gc alone");
    assertEquals(
        List.of("check"), process.steps().stream().map(StepDefinition::id).toList());
    assertEquals(PeerTarget.MAINTENANCE, process.steps().getFirst().target());
    assertEquals(List.of(), process.steps().getFirst().dependsOn());
  }

  @Test
  void theCheckIsABodylessPostWithItsOwnTimeoutAndTheReportIsTheSummary() {
    TimedPeers peers = peers();
    peers.answer(SbomCheckProcess.RUNS_PATH, Scripted.status(202, REPORT));
    StepResult result = check(peers);
    assertEquals(RunStatus.SUCCEEDED, result.status(), result.error());
    assertEquals(
        "2 versions without a usable SBOM, 1 ticket open, 1 warning", result.summary());
    assertEquals("POST", result.call().method());
    assertEquals("http://maintenance/maintenance/api/sbom-check/runs", result.call().url());
    assertNull(result.call().body(), "the door takes no body");
    assertEquals(List.of(TIMEOUT), peers.timeouts);
  }

  @Test
  void aReportOnlyRunSaysItFiledNothing() {
    FakePeers peers = ContractPeers.fake();
    peers.answer(
        SbomCheckProcess.RUNS_PATH,
        Scripted.status(
            202,
            "{\"ranAt\":\"2026-11-24T02:15:00Z\",\"filed\":false,\"entries\":[],"
                + "\"warnings\":[],\"tickets\":[]}"));
    StepResult result = check(peers);
    assertEquals(RunStatus.SUCCEEDED, result.status(), result.error());
    assertEquals(
        "0 versions without a usable SBOM, 0 tickets open, 0 warnings (report only: no tickets"
            + " filed)",
        result.summary());
  }

  @Test
  void anUnreachableArtifactsRegistryIsAFailedStep() {
    FakePeers peers = ContractPeers.fake();
    peers.answer(
        SbomCheckProcess.RUNS_PATH,
        Scripted.status(502, "{\"message\":\"qits-artifacts could not be asked\"}"));
    StepResult result = check(peers);
    assertEquals(RunStatus.FAILED, result.status());
    assertTrue(result.error().endsWith("answered 502"), result.error());
    assertTrue(result.answer().body().contains("could not be asked"), "the row keeps the answer");
  }
}
