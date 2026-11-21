package eu.wohlben.qits.orchestrator.process.gc;

import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * <b>qits-artifacts' cleanup listing, with the pins read here</b> (ticket qits-2138).
 *
 * <p>qits-artifacts-frontend lists its repositories with what a collection would reclaim, judged
 * against the platform's pin set. The five sources a gc run reads are private and qits-artifacts is
 * public, so it cannot read them; this service can. So the listing asks here, this service reads
 * the pins exactly as a gc run does ({@link GcPins}) and hands them to qits-artifacts'
 * supplied-pins listing as {@code {"pins": {...}}}. The same rule as the rest of this service:
 * <b>it only sends requests</b>.
 *
 * <pre>
 * repositories()  POST /artifacts/api/gc/repositories               {pins}
 * </pre>
 *
 * <p>The per-repository plan and sweep are gone (qits-2477): only the landing app's cleanup page
 * used them, and the daily gc does that work.
 *
 * <p><b>A pin source that could not be read is ABSENT from the body</b>, exactly as in a gc run's
 * body, and the request is still made: qits-artifacts reads a missing member as that source being
 * unanswered and says so in its answer.
 *
 * <p><b>The answer is qits-artifacts' own</b> — its {@link PeerExchange}, handed back to the
 * route untouched. No run row is written.
 */
@ApplicationScoped
public class ArtifactsCleanup {

  static final String REPOSITORIES_PATH = "/artifacts/api/gc/repositories";

  @Inject GcConfig config;

  @Inject PeerClient peers;

  /** The wait between two asks of a 503 pin read; a test outside CDI swaps in its own. */
  GcPins.Sleeper sleeper = GcPins.Sleeper.THREAD;

  /** Every repository, with what a collection of each would reclaim against the pins read now. */
  public PeerExchange repositories() {
    return forward(REPOSITORIES_PATH);
  }

  private PeerExchange forward(String path) {
    GcPins.Reading pins =
        GcPins.readAll(peers, config.pinsRetryAttempts(), config.pinsRetryDelay(), sleeper);
    return peers.post(PeerTarget.ARTIFACTS, path, pins.body());
  }
}
