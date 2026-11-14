package eu.wohlben.qits.orchestrator.process.gc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * <b>The five pin reads, and the {@code pins} object they make</b> — one copy, read by two callers
 * (ticket qits-2138).
 *
 * <p>{@link GcProcess} reads them as its five {@code pins.*} steps. The artifacts cleanup doors
 * ({@code ArtifactsCleanup}) read them on every request, because qits-artifacts' per-repository
 * cleanup UI can no longer read them itself: the five sources are private, qits-artifacts is not,
 * and this service is the one that holds a credential for all five and sits where they resolve.
 * Two readers of one keep-set must judge it identically, so what a source is — its peer, its path,
 * the name qits-artifacts takes it by, how its answer is judged and when it is asked again — lives
 * here and nowhere else. A source added to {@link Source} is a source both callers read.
 *
 * <p><b>Nothing here is a step.</b> {@link #read} answers the {@link StepResult} a step would have
 * written, and {@link #pinsObject} takes the answers from wherever its caller keeps them — the run's
 * {@code RunContext} for a gc run, a {@link Reading} for a door. The step framework, its edges and
 * its call recording stay {@link GcProcess}'s own.
 */
public final class GcPins {

  private static final ObjectMapper JSON = new ObjectMapper();

  /**
   * One pin source: the step id a gc run records it under, the step's name, the peer and path it is
   * read from, the member it travels as in {@code {"pins": {...}}}, and the summariser of its
   * answer. Declared in the order the gc process reads them, which is the order every body lists
   * them in.
   */
  public enum Source {
    DEPLOYMENTS(
        GcProcess.PINS_DEPLOYMENTS,
        "Deployment pins",
        PeerTarget.DEPLOYMENTS,
        "/deployments/api/pins",
        "deployments",
        answer -> GcSummaries.deploymentPins(answer.json())),
    CI(
        GcProcess.PINS_CI,
        "CI daemon pin",
        PeerTarget.CI,
        "/ci/api/daemon",
        "ciDaemon",
        answer -> GcSummaries.ciPin(answer.json())),
    DEPENDENCIES(
        GcProcess.PINS_DEPENDENCIES,
        "Dependency pins",
        PeerTarget.MAINTENANCE,
        "/maintenance/api/pins",
        "dependencies",
        answer -> GcSummaries.dependencyPins(answer.json())),
    WORKSPACES(
        GcProcess.PINS_WORKSPACES,
        "Workspace launch pins",
        PeerTarget.WORKSPACES,
        "/workspaces/api/pins",
        "workspaceLaunches",
        answer -> GcSummaries.workspaceLaunchPins(answer.json())),
    PROJECTS(
        GcProcess.PINS_PROJECTS,
        "Project launch pins",
        PeerTarget.PROJECTS,
        "/projects/api/pins",
        "projectLaunches",
        answer -> GcSummaries.projectLaunchPins(answer.json()));

    private final String stepId;
    private final String stepName;
    private final String target;
    private final String path;
    private final String member;
    private final Function<PeerAnswer, String> summary;

    Source(
        String stepId,
        String stepName,
        String target,
        String path,
        String member,
        Function<PeerAnswer, String> summary) {
      this.stepId = stepId;
      this.stepName = stepName;
      this.target = target;
      this.path = path;
      this.member = member;
      this.summary = summary;
    }

    /** The gc step that reads this source: {@code pins.deployments}, … */
    public String stepId() {
      return stepId;
    }

    /** The gc step's human name. */
    public String stepName() {
      return stepName;
    }

    /** The {@link PeerTarget} that answers it. */
    public String target() {
      return target;
    }

    /** The path read on that peer. */
    public String path() {
      return path;
    }

    /** The member of {@code {"pins": {...}}} its answer travels as. */
    public String member() {
      return member;
    }
  }

  /**
   * How a pin read waits between two asks. A seam rather than a {@code Thread.sleep} inline, so a
   * suite that drives the shipped thirty-second delay never actually waits it out.
   */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    Sleeper THREAD = duration -> Thread.sleep(duration.toMillis());
  }

  /**
   * One pin read: a GET judged by {@link StepResult#ofKeepSet}, asked again while the peer answers
   * 503 (ticket qits-1733).
   *
   * <p><b>503 alone, because 503 alone says "ask again".</b> qits-maintenance answers it while its
   * pin set is not complete, and its body says the next read answers once it is; the heal lands in
   * seconds. Any other non-2xx, a peer that cannot be reached, and a 2xx that cannot be read whole
   * are answers about the peer rather than about a moment, so they fail on the first read as they
   * always have. Up to {@code attempts} reads in all, {@code delay} apart; the last 503 FAILS the
   * read, and the caller does the rest — a gc run's edges, a door's absent member.
   *
   * <p><b>The result is the LAST read's</b> — its call, its answer, its verdict — with the count
   * beside it when there was more than one: {@code (after 2 attempts)} on the summary, {@code (6
   * attempts, 30s apart)} after the error. The error keeps its own text as the prefix, so {@code
   * answered 503} still reads the way every reader of a failed pin row already matches it.
   *
   * <p>An interrupt stops the asking: the flag is restored and the last read is the verdict.
   */
  public static StepResult read(
      PeerClient peers, Source source, int attempts, Duration delay, Sleeper sleeper) {
    StepResult result = once(peers, source);
    int made = 1;
    while (made < attempts && answered503(result)) {
      try {
        sleeper.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      result = once(peers, source);
      made++;
    }
    if (made == 1) {
      return result;
    }
    if (result.status() == RunStatus.SUCCEEDED) {
      return new StepResult(
          result.status(),
          result.call(),
          result.answer(),
          result.summary() + " (after " + made + " attempts)",
          result.error(),
          result.skip());
    }
    return new StepResult(
        result.status(),
        result.call(),
        result.answer(),
        result.summary(),
        result.error() + " (" + made + " attempts, " + spelled(delay) + " apart)",
        result.skip());
  }

  private static StepResult once(PeerClient peers, Source source) {
    return StepResult.ofKeepSet(peers.get(source.target(), source.path()), source.summary);
  }

  /** A FAILED read whose peer answered, and answered 503 — the one answer worth asking again. */
  private static boolean answered503(StepResult result) {
    PeerAnswer answer = result.answer();
    return result.status() == RunStatus.FAILED
        && answer != null
        && answer.error() == null
        && Integer.valueOf(503).equals(answer.httpStatus());
  }

  /** {@code 30s}, or {@code 250ms} for a delay that is not whole seconds. */
  static String spelled(Duration delay) {
    long millis = delay.toMillis();
    return millis % 1000 == 0 ? (millis / 1000) + "s" : millis + "ms";
  }

  /**
   * The five pin answers, re-embedded verbatim under the names qits-artifacts (and qits-projects'
   * tag sweep) take them by:
   *
   * <pre>
   * {"deployments":      &lt;the deployments answer, verbatim&gt;,
   *  "ciDaemon":         &lt;the ci answer, verbatim&gt;,
   *  "dependencies":     &lt;the maintenance answer, verbatim&gt;,
   *  "workspaceLaunches":&lt;the workspaces answer, verbatim&gt;,
   *  "projectLaunches":  &lt;the projects answer, verbatim&gt;}
   * </pre>
   *
   * <p><b>Verbatim means re-embedded rather than re-shaped</b>: the five contracts belong to
   * qits-deployments, qits-ci, qits-maintenance, qits-workspaces and qits-projects, and a projection
   * here would be a sixth copy of them to keep in step.
   *
   * <p><b>A member with no answer is ABSENT, not null.</b> qits-artifacts reads a missing member as
   * that pin source being unanswered, which makes a plan not executable and aborts a sweep — the
   * fail-closed rule, judged by the owner of the store.
   *
   * @param answer the parsed answer of each source's step id, empty when it has none to give
   */
  public static ObjectNode pinsObject(Function<String, Optional<JsonNode>> answer) {
    ObjectNode pins = JSON.createObjectNode();
    for (Source source : Source.values()) {
      answer.apply(source.stepId()).ifPresent(node -> pins.set(source.member(), node));
    }
    return pins;
  }

  /** {@code {"pins": }{@link #pinsObject}{@code }}, as text — the body qits-artifacts takes. */
  public static String pinsBody(Function<String, Optional<JsonNode>> answer) {
    ObjectNode body = JSON.createObjectNode();
    body.set("pins", pinsObject(answer));
    return body.toString();
  }

  /**
   * All five read once, in {@link Source} order, and the body they make: what a caller outside a
   * gc run hands qits-artifacts.
   */
  public static Reading readAll(PeerClient peers, int attempts, Duration delay, Sleeper sleeper) {
    List<Read> reads = new ArrayList<>();
    for (Source source : Source.values()) {
      reads.add(new Read(source, read(peers, source, attempts, delay, sleeper)));
    }
    return new Reading(List.copyOf(reads));
  }

  /** One source and what reading it came to. */
  public record Read(Source source, StepResult result) {

    /** Whether the read SUCCEEDED, which is the only way its answer reaches a body. */
    public boolean succeeded() {
      return result.status() == RunStatus.SUCCEEDED;
    }
  }

  /** The five reads of one moment. */
  public record Reading(List<Read> reads) {

    /**
     * A source's answer, when its read SUCCEEDED. A failed read's answer — a 503 body, an error
     * object — is never a pin set, so it is absent here and absent from {@link #body}.
     */
    public Optional<JsonNode> answer(String stepId) {
      return reads.stream()
          .filter(read -> read.source().stepId().equals(stepId) && read.succeeded())
          .findFirst()
          .map(read -> read.result().answer().json());
    }

    /** {@code {"pins": {...}}}, every member that could not be read left out. */
    public String body() {
      return pinsBody(this::answer);
    }

    /** The sources that could not be read, in order: what a log line names. */
    public List<Read> failed() {
      return reads.stream().filter(read -> !read.succeeded()).toList();
    }
  }

  private GcPins() {}
}
