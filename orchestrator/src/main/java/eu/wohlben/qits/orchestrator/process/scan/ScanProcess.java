package eu.wohlben.qits.orchestrator.process.scan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The scan process (ticket qits-2490): qits-maintenance's nightly manifest scans, run here so they
 * have a run history, a Run now and events — until then they were two crons inside qits-maintenance
 * whose only account of a failed night was a log line.
 *
 * <h2>What it does</h2>
 *
 * Two scans, one after the other, each in two steps:
 *
 * <ul>
 *   <li>{@code internal.request} — {@code POST /maintenance/api/scans {"scope":"INTERNAL"}}, which
 *       queues a scan of the whole catalogue's own artifacts and answers 202 {@code {id}};
 *   <li>{@code internal.follow} — {@code GET /maintenance/api/scans/{id}} every {@link
 *       ScanConfig#pollInterval()} until the scan ends: SUCCEEDED is a SUCCEEDED step, FAILED a
 *       FAILED step carrying the scan's own {@code message}, and a scan still REQUESTED or RUNNING
 *       after {@link ScanConfig#followTimeout()} a FAILED step saying so. The row keeps the last
 *       read;
 *   <li>{@code external.request} / {@code external.follow} — the same with {@code EXTERNAL}.
 * </ul>
 *
 * <p><b>External does not depend on internal.</b> The two scans read different registries and
 * neither needs the other's answer, so a broken internal scan must not cost the night its external
 * one. Declaration order still runs it second — which is what replaces the half hour the two crons
 * used to sit apart: qits-maintenance runs one scan at a time, and a follow that waits for the
 * first to end is a better spacing than a guess.
 *
 * <p><b>No dry run</b> ({@link #dryRunnable()}): the door has no such mode. And the scan rows
 * qits-maintenance writes say {@code trigger=MANUAL} — its door takes no trigger, and the run here
 * is where SCHEDULED or MANUAL is recorded.
 */
@ApplicationScoped
public class ScanProcess implements TechnicalProcess {

  /** The kind, in every url and in every stored row. */
  public static final String KIND = "scan";

  public static final String INTERNAL_REQUEST = "internal.request";
  public static final String INTERNAL_FOLLOW = "internal.follow";
  public static final String EXTERNAL_REQUEST = "external.request";
  public static final String EXTERNAL_FOLLOW = "external.follow";

  /** qits-maintenance's scan door: POST queues one, {@code /{id}} reads it. */
  public static final String SCANS_PATH = "/maintenance/api/scans";

  private static final ObjectMapper JSON = new ObjectMapper();

  /** How a follow step waits between two reads — swapped by a test so it never really waits. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    Sleeper THREAD = duration -> Thread.sleep(duration.toMillis());
  }

  @Inject ScanConfig config;

  Sleeper sleeper = Sleeper.THREAD;

  /** What "after the follow timeout" is measured on — a test moves it as its sleeper waits. */
  Clock clock = Clock.systemUTC();

  /** The process outside CDI, for a test: its settings, its wait and its clock. */
  public static ScanProcess of(ScanConfig config, Sleeper sleeper, Clock clock) {
    ScanProcess process = new ScanProcess();
    process.config = config;
    process.sleeper = sleeper;
    process.clock = clock;
    return process;
  }

  @Override
  public String kind() {
    return KIND;
  }

  @Override
  public String name() {
    return "Manifest scan";
  }

  @Override
  public String description() {
    return "Asks qits-maintenance to re-scan every repository's manifests and refresh the latest"
        + " versions: first the platform's own artifacts (INTERNAL), then everything published"
        + " elsewhere (EXTERNAL), each followed until it ends. Bumps nothing — what is out of"
        + " date is what the next release request's pre-run bumps.";
  }

  @Override
  public boolean dryRunnable() {
    return false;
  }

  @Override
  public List<StepDefinition> steps() {
    return List.of(
        request(INTERNAL_REQUEST, "Request the internal scan", "INTERNAL"),
        follow(INTERNAL_FOLLOW, "Follow the internal scan", INTERNAL_REQUEST),
        // NO edge on the internal pair: an internal scan that failed says nothing about the
        // external registries, and declaration order alone puts this one second.
        request(EXTERNAL_REQUEST, "Request the external scan", "EXTERNAL"),
        follow(EXTERNAL_FOLLOW, "Follow the external scan", EXTERNAL_REQUEST));
  }

  /** The request body for one scope: {@code {"scope":"<scope>"}}, the whole catalogue. */
  static String requestBody(String scope) {
    return JSON.createObjectNode().put("scope", scope).toString();
  }

  private static StepDefinition request(String id, String name, String scope) {
    return new StepDefinition(
        id, name, PeerTarget.MAINTENANCE, List.of(), context -> requestScan(context, scope));
  }

  private StepDefinition follow(String id, String name, String requestStep) {
    return new StepDefinition(
        id,
        name,
        PeerTarget.MAINTENANCE,
        List.of(requestStep),
        context -> follow(context, requestStep));
  }

  /**
   * Queues one scan. A 2xx without an id is FAILED here rather than at the follow: a queued scan
   * nobody can follow is a scan this run cannot account for.
   */
  static StepResult requestScan(RunContext context, String scope) {
    PeerExchange exchange =
        context.peers().post(PeerTarget.MAINTENANCE, SCANS_PATH, requestBody(scope));
    StepResult result = StepResult.of(exchange, answer -> ScanSummaries.requested(answer.json()));
    if (result.status() == RunStatus.SUCCEEDED && scanId(exchange.answer().json()).isEmpty()) {
      return new StepResult(
          RunStatus.FAILED,
          exchange.call(),
          exchange.answer(),
          null,
          exchange.call().url() + " answered " + exchange.answer().httpStatus() + " with no scan id",
          null);
    }
    return result;
  }

  /**
   * Reads the scan its request step queued until it ends, or until {@link
   * ScanConfig#followTimeout()} has passed. Never throws; an interrupt ends the wait as a FAILED
   * step and keeps the flag.
   */
  StepResult follow(RunContext context, String requestStep) {
    Optional<String> id = context.answer(requestStep).flatMap(ScanProcess::scanId);
    if (id.isEmpty()) {
      return StepResult.refused(requestStep + " handed over no scan id to follow");
    }
    String path = SCANS_PATH + "/" + id.get();
    Duration timeout = config.followTimeout();
    Instant deadline = clock.instant().plus(timeout);
    while (true) {
      PeerExchange exchange = context.peers().get(PeerTarget.MAINTENANCE, path);
      StepResult read = StepResult.of(exchange, answer -> ScanSummaries.finished(answer.json()));
      if (read.status() != RunStatus.SUCCEEDED) {
        return read; // the read itself failed: a status, an unreachable peer
      }
      JsonNode scan = exchange.answer().json();
      String status = scan == null ? null : scan.path("status").asText(null);
      if ("SUCCEEDED".equals(status)) {
        return read;
      }
      if ("FAILED".equals(status)) {
        String message = scan.path("message").asText(null);
        return failed(
            exchange,
            "scan "
                + id.get()
                + " FAILED: "
                + (message == null || message.isBlank() ? "qits-maintenance gave no message" : message));
      }
      if (!"REQUESTED".equals(status) && !"RUNNING".equals(status)) {
        return failed(
            exchange,
            exchange.call().url()
                + " answered a scan whose status is "
                + (status == null ? "absent" : "'" + status + "'"));
      }
      if (!clock.instant().isBefore(deadline)) {
        return failed(
            exchange,
            "scan "
                + id.get()
                + " is still "
                + status
                + " after "
                + timeout
                + " (qits.orchestrator.scan.follow-timeout); it is left to qits-maintenance");
      }
      try {
        sleeper.sleep(config.pollInterval());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return failed(
            exchange, "following scan " + id.get() + " was interrupted while it was " + status);
      }
    }
  }

  private static StepResult failed(PeerExchange exchange, String error) {
    return new StepResult(RunStatus.FAILED, exchange.call(), exchange.answer(), null, error, null);
  }

  private static Optional<String> scanId(JsonNode answer) {
    if (answer == null) {
      return Optional.empty();
    }
    String id = answer.path("id").asText(null);
    return id == null || id.isBlank() ? Optional.empty() : Optional.of(id);
  }
}
