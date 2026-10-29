package eu.wohlben.qits.orchestrator.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.error.NoSuchProcessException;
import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The executor: order, the skip cascade, and the fact that every transition is on disk before the
 * run ends.
 *
 * <p>Nothing here calls a peer — {@link ProbeProcess}'s steps decide their own verdict — so what is
 * under test is the executor and not the gc definition. {@code GcProcessTest} is the other half.
 */
@QuarkusTest
class RunExecutorTest {

  @Inject RunExecutor executor;

  @Inject RunStore runs;

  /**
   * A poll, the way a deployment does it.
   *
   * <p><b>The clear is load-bearing.</b> A {@code @QuarkusTest} holds ONE request context for the
   * whole test method, so one Hibernate session serves every read here and would answer every poll
   * out of its first-level cache — the run row would stay RUNNING forever while the worker thread
   * closed it in another session. In a deployment each poll is a fresh HTTP request with a session
   * of its own; clearing is how the suite gets the same thing.
   */
  private void nextPoll() {
    runs.getEntityManager().clear();
  }

  /** A run is asynchronous; every assertion below waits for it to close first. */
  private OpRun awaitClosed(UUID id) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
    while (Instant.now().isBefore(deadline)) {
      nextPoll();
      OpRun run = runs.run(id).orElseThrow();
      if (!RunStatus.RUNNING.name().equals(run.status)) {
        return run;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new AssertionError("run " + id + " never finished");
  }

  private Map<String, OpStep> stepsOf(UUID id) {
    nextPoll();
    Map<String, OpStep> byId = new LinkedHashMap<>();
    runs.steps(id).forEach(step -> byId.put(step.stepId, step));
    return byId;
  }

  @Test
  void stepsRunInDeclarationOrderAndEveryTransitionIsPersisted() {
    ProbeProcess.script =
        List.of(
            ProbeProcess.succeeding("first"),
            ProbeProcess.succeeding("second", "first"),
            ProbeProcess.succeeding("third", "second"));

    UUID id = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.SUCCEEDED.name(), run.status);
    assertEquals("manual", run.trigger);
    assertNotNull(run.finishedAt);
    assertTrue(run.summary.contains("first SUCCEEDED"), run.summary);

    List<OpStep> steps = List.copyOf(stepsOf(id).values());
    assertEquals(List.of("first", "second", "third"), steps.stream().map(step -> step.stepId).toList());
    assertEquals(List.of(0, 1, 2), steps.stream().map(step -> step.seq).toList());
    for (OpStep step : steps) {
      assertEquals(RunStatus.SUCCEEDED.name(), step.status, step.stepId);
      // Both timestamps are written by the executor at the two transitions, so a row that has one
      // and not the other is a step whose RUNNING write never happened.
      assertNotNull(step.startedAt, step.stepId);
      assertNotNull(step.finishedAt, step.stepId);
      assertEquals(step.stepId + " ok", step.summary);
    }
    // The edges are copied into the row at open, so a run stays readable after the definition moves.
    assertEquals("first", stepsOf(id).get("second").dependsOn);
    assertNull(stepsOf(id).get("first").dependsOn);
  }

  @Test
  void aFailedStepSkipsEveryTransitiveDependentAndLeavesTheRestAlone() {
    ProbeProcess.script =
        List.of(
            ProbeProcess.succeeding("root"),
            ProbeProcess.failing("broken", "root"),
            ProbeProcess.succeeding("child", "broken"),
            ProbeProcess.succeeding("grandchild", "child"),
            ProbeProcess.succeeding("independent", "root"));

    UUID id = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    // Any FAILED step fails the run; a SKIPPED one does not, because a skip is the consequence of a
    // failure already counted.
    assertEquals(RunStatus.FAILED.name(), run.status);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("root").status);
    assertEquals(RunStatus.FAILED.name(), steps.get("broken").status);
    assertEquals("broken broke", steps.get("broken").error);

    // THE CASCADE NAMES THE STEP THAT ACTUALLY FAILED, not the skipped neighbour in between — a
    // reader should not have to walk the graph backwards to find the cause.
    assertEquals(RunStatus.SKIPPED.name(), steps.get("child").status);
    assertEquals("skipped: broken failed", steps.get("child").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("grandchild").status);
    assertEquals("skipped: broken failed", steps.get("grandchild").error);

    // AND AN INDEPENDENT STEP STILL RUNS. This is the whole reason the edges are per step: a night
    // where one broken peer stopped every unrelated reclaim would be a night of no reclaim for no
    // reason.
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("independent").status);

    // A skipped step made no call, so it has no request and no answer to record.
    assertNull(steps.get("child").requestUrl);
    assertNull(steps.get("child").httpStatus);
    assertNull(steps.get("child").startedAt);
  }

  @Test
  void aStepTheProcessChoseNotToMakeDoesNotSkipWhatComesAfterIt() {
    // The shape of a dry run: the step that would delete is skipped by POLICY, and the step that
    // measures afterwards depends on it.
    ProbeProcess.script =
        List.of(
            ProbeProcess.succeeding("before"),
            ProbeProcess.policySkipping("deleting", "before"),
            ProbeProcess.succeeding("after", "deleting"),
            ProbeProcess.succeeding("downstream", "after"));

    UUID id = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    // MEASURED LIVE: the platform's first real dry run answered 200 to all nine calls and still
    // reported its last step as "skipped: artifacts.sweep failed". A policy skip is the run doing
    // what it was asked, so nothing after it has a reason not to run.
    assertEquals(RunStatus.SUCCEEDED.name(), run.status);

    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("deleting").status);
    assertEquals("dry run", steps.get("deleting").error);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("after").status);
    assertEquals(RunStatus.SUCCEEDED.name(), steps.get("downstream").status);
    assertNull(steps.get("after").error);
  }

  @Test
  void aSecondRunOfTheSameKindIsRefusedWhileOneIsActive() {
    // A step that never returns until the test lets it, so the first run is provably still RUNNING
    // when the second is attempted.
    java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
    ProbeProcess.script =
        List.of(
            new eu.wohlben.qits.orchestrator.process.StepDefinition(
                "held",
                "held",
                eu.wohlben.qits.orchestrator.peer.PeerTarget.CONTAINERS,
                List.of(),
                context -> {
                  started.countDown();
                  try {
                    gate.await(30, java.util.concurrent.TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return new StepResultBuilder().succeeded("released");
                }));

    UUID first = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    try {
      assertTrue(
          started.await(30, java.util.concurrent.TimeUnit.SECONDS), "the held step never started");
      RunAlreadyActiveException refused =
          assertThrows(
              RunAlreadyActiveException.class,
              () -> executor.start(ProbeProcess.KIND, RunTrigger.SCHEDULED, false));
      assertEquals(409, refused.statusCode());
      assertEquals(first, refused.activeRunId());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } finally {
      gate.countDown();
    }
    assertEquals(RunStatus.SUCCEEDED.name(), awaitClosed(first).status);
  }

  @Test
  void aRunWhoseProcessDiedIsAbandonedOnceItIsOlderThanTheLimitAndNoLonger() {
    // A run opened and never executed is what a crashed process leaves: a RUNNING row nothing will
    // ever close (qits-1663).
    ProbeProcess.script = List.of(ProbeProcess.succeeding("first"), ProbeProcess.succeeding("second"));
    UUID dead =
        runs.open(
            ProbeProcess.KIND,
            RunTrigger.SCHEDULED,
            false,
            ProbeProcess.script,
            Instant.now().minus(Duration.ofHours(4)));
    runs.stepRunning(dead, "first", Instant.now().minus(Duration.ofHours(4)));

    UUID next = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    assertEquals(RunStatus.SUCCEEDED.name(), awaitClosed(next).status);

    nextPoll();
    OpRun abandoned = runs.run(dead).orElseThrow();
    assertEquals(RunStatus.FAILED.name(), abandoned.status);
    assertTrue(abandoned.summary.startsWith("abandoned"), abandoned.summary);
    assertNull(abandoned.finishedAt);
    Map<String, OpStep> steps = stepsOf(dead);
    assertEquals(RunStatus.FAILED.name(), steps.get("first").status);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("second").status);
  }

  @Test
  void aRunYoungerThanTheAbandonLimitStillRefusesASecond() {
    ProbeProcess.script = List.of(ProbeProcess.succeeding("only"));
    UUID young =
        runs.open(
            ProbeProcess.KIND,
            RunTrigger.SCHEDULED,
            false,
            ProbeProcess.script,
            Instant.now().minus(Duration.ofMinutes(30)));
    try {
      RunAlreadyActiveException refused =
          assertThrows(
              RunAlreadyActiveException.class,
              () -> executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false));
      assertEquals(young, refused.activeRunId());
    } finally {
      runs.close(young, RunStatus.FAILED, "closed by the test", Instant.now());
    }
  }

  @Test
  void anUnknownKindIsRefusedBeforeAnyRowIsWritten() {
    assertThrows(
        NoSuchProcessException.class,
        () -> executor.start("no-such-process", RunTrigger.MANUAL, false));
  }

  @Test
  void aStepThatThrowsIsAFailedRowRatherThanADeadRun() {
    ProbeProcess.script =
        List.of(
            new eu.wohlben.qits.orchestrator.process.StepDefinition(
                "throws",
                "throws",
                eu.wohlben.qits.orchestrator.peer.PeerTarget.CONTAINERS,
                List.of(),
                context -> {
                  throw new IllegalStateException("the body blew up");
                }),
            ProbeProcess.succeeding("after", "throws"));

    UUID id = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
    OpRun run = awaitClosed(id);

    assertEquals(RunStatus.FAILED.name(), run.status);
    Map<String, OpStep> steps = stepsOf(id);
    assertEquals(RunStatus.FAILED.name(), steps.get("throws").status);
    assertTrue(steps.get("throws").error.contains("the body blew up"), steps.get("throws").error);
    assertEquals(RunStatus.SKIPPED.name(), steps.get("after").status);
  }

  @Test
  void theTriggerIsStoredAsTheWordTheApiServes() {
    ProbeProcess.script = List.of(ProbeProcess.succeeding("only"));
    UUID id = executor.start(ProbeProcess.KIND, RunTrigger.SCHEDULED, true);
    OpRun run = awaitClosed(id);
    assertEquals("scheduled", run.trigger);
    assertTrue(run.dryRun);
  }

  /**
   * qits-1733: a FAILED run is a WARN naming each failed step and its peer. At INFO, twelve gc runs
   * in a week deleted nothing and nobody saw it. Captured at the executor's own logger, the way any
   * handler under the JBoss log manager would see it.
   */
  @Test
  void aFailedRunLogsAWarnNamingEachFailedStepAndItsPeer() throws InterruptedException {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger(RunExecutor.class.getName());
    java.util.List<java.util.logging.LogRecord> records =
        new java.util.concurrent.CopyOnWriteArrayList<>();
    java.util.logging.Handler capture =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(capture);
    try {
      ProbeProcess.script =
          List.of(
              ProbeProcess.succeeding("root"),
              ProbeProcess.failing("broken", "root"),
              ProbeProcess.succeeding("child", "broken"));
      UUID id = executor.start(ProbeProcess.KIND, RunTrigger.MANUAL, false);
      awaitClosed(id);

      // The line is written after the row is closed, so it may land a moment after the poll.
      String expected = "probe run " + id + " FAILED: broken (containers): broken broke";
      Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
      java.util.logging.LogRecord line = null;
      while (line == null && Instant.now().isBefore(deadline)) {
        line =
            records.stream()
                .filter(record -> record.getMessage().contains(id.toString()))
                .findFirst()
                .orElse(null);
        if (line == null) {
          Thread.sleep(20);
        }
      }
      assertNotNull(line, "the failed run was logged");
      assertEquals(java.util.logging.Level.WARNING.intValue(), line.getLevel().intValue());
      assertEquals(expected, line.getMessage());
    } finally {
      logger.removeHandler(capture);
    }
  }

  @Test
  void aFailedStepsWarnClauseCarriesItsPeerItsErrorAndTheStartOfTheBody() {
    var step = ProbeProcess.failing("pins.dependencies");
    String body = "{\"message\":\"" + "x".repeat(400) + "\"}";
    var result =
        new eu.wohlben.qits.orchestrator.process.StepResult(
            RunStatus.FAILED,
            null,
            new eu.wohlben.qits.orchestrator.peer.PeerAnswer(503, body, null, null),
            null,
            "http://dev-qits-maintenance:8080/maintenance/api/pins answered 503 (6 attempts, 30s apart)",
            null);

    String clause = RunExecutor.failure(step, result);

    String prefix =
        "pins.dependencies (containers): http://dev-qits-maintenance:8080/maintenance/api/pins"
            + " answered 503 (6 attempts, 30s apart): ";
    assertTrue(clause.startsWith(prefix), clause);
    assertEquals(body.substring(0, RunExecutor.FAILURE_BODY_CHARS) + "…", clause.substring(prefix.length()));
    assertEquals(
        "pins.ci (containers): unreachable",
        RunExecutor.failure(
            ProbeProcess.failing("pins.ci"),
            new StepResultBuilder().failed("unreachable")));
  }
}
