package eu.wohlben.qits.orchestrator.process.gc;

import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 * <b>qits-artifacts' per-repository cleanup, with the pins read here</b> (ticket qits-2138).
 *
 * <p>qits-artifacts' cleanup UI lists its repositories with what a collection would reclaim, shows
 * one repository's plan, and sweeps one repository. Each of those judges against the platform's pin
 * set, and qits-artifacts used to read that set itself, over the network, from the five sources a
 * gc run reads. Those sources are private now and qits-artifacts is public, so it cannot reach them
 * — and this service can: it is private, and it already reads all five for every gc run. So the UI
 * asks this service, this service reads the pins exactly as a gc run does ({@link GcPins}) and hands
 * them to qits-artifacts' supplied-pins doors as {@code {"pins": {...}}}, and qits-artifacts judges
 * as it always has. The same rule as the rest of this service: <b>it only sends requests</b>, and
 * every decision about what may be deleted stays with the store's owner.
 *
 * <pre>
 * repositories()  POST /artifacts/api/gc/repositories               {pins}
 * plan(r)         POST /artifacts/api/gc/repositories/{r}/plan      {pins}
 * sweep(r)        POST /artifacts/api/gc/repositories/{r}/sweep     {pins}   — through {@link GcExclusion}
 * </pre>
 *
 * <p><b>A pin source that could not be read is ABSENT from the body</b>, exactly as in a gc run's
 * body, and the request is still made: qits-artifacts reads a missing member as that source being
 * unanswered, so the plan it answers says it is not executable and names the source, and a sweep
 * aborts. Refusing here instead would give the UI a bare error where the owner's own answer says
 * which source is missing and what that withholds.
 *
 * <p><b>The answer is qits-artifacts' own</b> — its {@link PeerExchange}, handed back to the
 * route untouched. No run row is written: a cleanup request is a person's question to qits-artifacts
 * that happens to need credentials only this service holds, not a technical process.
 */
@ApplicationScoped
public class ArtifactsCleanup {

  private static final Logger LOG = Logger.getLogger(ArtifactsCleanup.class);

  static final String REPOSITORIES_PATH = "/artifacts/api/gc/repositories";

  @Inject GcConfig config;

  @Inject PeerClient peers;

  @Inject GcExclusion exclusion;

  /** The wait between two asks of a 503 pin read; a test outside CDI swaps in its own. */
  GcPins.Sleeper sleeper = GcPins.Sleeper.THREAD;

  /**
   * What one request came to: the pins as they were read, and qits-artifacts' exchange — its status
   * and its body, or the sentence saying why it could not be asked. The doors hand on the exchange
   * alone; the reading stays here, for the sweep's log line.
   */
  record Forwarded(GcPins.Reading pins, PeerExchange exchange) {}

  /** Every repository, with what a collection of each would reclaim against the pins read now. */
  public PeerExchange repositories() {
    return forward(REPOSITORIES_PATH).exchange();
  }

  /** One repository's plan against the pins read now. Nothing is deleted. */
  public PeerExchange plan(String repository) {
    return forward(repositoryPath(repository, "plan")).exchange();
  }

  /**
   * Sweeps one repository against the pins read now — refused while a gc run is active, and
   * holding off every gc start until it answers ({@link GcExclusion#sweeping}). The pins are read
   * inside that window, so no gc run deletes between the read and the sweep that judges by it.
   *
   * <p>One INFO line per sweep: the repository, who asked, and the receipt's figures as
   * qits-artifacts reported them — or its status, or why it could not be reached.
   *
   * @param caller the principal that asked, for the log line; null when there is none
   * @throws eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException a gc run is active — 409
   */
  public PeerExchange sweep(String repository, String caller) {
    return exclusion.sweeping(
        repository,
        () -> {
          Forwarded forwarded = forward(repositoryPath(repository, "sweep"));
          LOG.infof(
              "artifacts cleanup sweep of %s by %s: %s%s",
              repository,
              caller == null || caller.isBlank() ? "an unnamed caller" : caller,
              outcome(forwarded.exchange().answer()),
              unread(forwarded.pins()));
          return forwarded.exchange();
        });
  }

  private Forwarded forward(String path) {
    GcPins.Reading pins =
        GcPins.readAll(peers, config.pinsRetryAttempts(), config.pinsRetryDelay(), sleeper);
    return new Forwarded(pins, peers.post(PeerTarget.ARTIFACTS, path, pins.body()));
  }

  /**
   * {@code /artifacts/api/gc/repositories/<repository>/<action>}, the name encoded as ONE path
   * segment: a {@code /} or a {@code ..} in it addresses nothing but a repository of that name.
   */
  static String repositoryPath(String repository, String action) {
    String segment = URLEncoder.encode(repository, StandardCharsets.UTF_8).replace("+", "%20");
    return REPOSITORIES_PATH + "/" + segment + "/" + action;
  }

  /**
   * The receipt's own totals, read exactly as a gc run's {@code artifacts.sweep} line reads them
   * ({@link GcSummaries#artifactsSweep}: blobs unlinked and bytes reclaimed, or why it aborted) —
   * the fields the consumer pact row binds, and nothing it does not.
   */
  private static String outcome(PeerAnswer answer) {
    if (answer.error() != null) {
      return "qits-artifacts could not be asked: " + answer.error();
    }
    if (!answer.ok()) {
      return "qits-artifacts answered " + answer.httpStatus();
    }
    String line;
    try {
      line = GcSummaries.artifactsSweep(answer.json());
    } catch (RuntimeException e) {
      line = "the receipt could not be read: " + e;
    }
    return "answered " + answer.httpStatus() + ", " + line;
  }

  /** {@code ; pins not read: pins.ci (…)}, or nothing when all five were. */
  private static String unread(GcPins.Reading pins) {
    if (pins.failed().isEmpty()) {
      return "";
    }
    return "; pins not read: "
        + pins.failed().stream()
            .map(read -> read.source().stepId() + " (" + read.result().error() + ")")
            .collect(Collectors.joining(", "));
  }
}
