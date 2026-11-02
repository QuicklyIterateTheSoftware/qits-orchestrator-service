package eu.wohlben.qits.orchestrator.stories.support;

import eu.wohlben.qits.servicemock.idp.MockIdp;
import eu.wohlben.qits.userflows.NetworkCapture;
import eu.wohlben.qits.userflows.NetworkEdge;
import eu.wohlben.qits.userflows.NetworkTaps;

/**
 * <b>Both ends of every diagram in this catalogue, wired in one call</b> — so a story class's
 * {@code @BeforeAll} is one line and no class can wire half of it.
 *
 * <p>There are three feeds and they are three different mechanisms:
 *
 * <ul>
 *   <li><b>the near side</b>, {@link NetworkTaps#restAssured}: every request a story makes becomes
 *       {@code <actor> -> qits-platform-orchestrator}. The framework ships it; this repository's
 *       hand-copied {@code StoryNetworkFilter} was deleted when these stories were written. It is
 *       idempotent per service, which is why every class may call this method;
 *   <li><b>the idp's recording</b>, cumulative and <b>with no floor</b>: the discovery read and
 *       JWKS fetch this service makes at STARTUP happen before any story exists and are the whole
 *       subject of the first one, so they must be attributable rather than filtered away;
 *   <li><b>the peers' access log</b>, {@link StoryPeers#install()} — cumulative, floored, and the
 *       only place the outgoing half of a run exists at all.
 * </ul>
 *
 * <p><b>Order is load-bearing and it is the package names that carry it.</b> A cumulative source is
 * attributed by a cursor, so pre-story traffic lands in whichever story drains FIRST. {@code
 * UserflowClassOrderer} sorts by fully-qualified class name, so {@code …orchestrator.api} runs
 * before {@code …orchestrator.stories.*} and the boot story owns the discovery read and JWKS fetch; within {@code
 * stories}, {@code collection} runs before {@code faults}, {@code operations} and {@code refusals},
 * which is why the first run of the catalogue — and with it the eight outbound token mints — belongs
 * to {@code GarbageCollectionRunIT}. {@code @UserflowRunsAfter} states the ones that are real
 * dependencies as well as being true of the names.
 */
public final class StoryNetwork {

  /** The id the idp's cumulative recording is registered under. Re-registering keeps its cursor. */
  private static final String IDP_SOURCE = "mock-idp";

  private StoryNetwork() {}

  /**
   * Install the near-side tap and register both far-side recordings. Idempotent, and safe from any
   * story class's {@code @BeforeAll} — {@link NetworkCapture#source} replaces a supplier while
   * keeping its cursor, so a class that runs second does not re-attribute what the first drained.
   */
  public static void install() {
    NetworkTaps.restAssured(StoryTarget.SERVICE);
    NetworkCapture.source(
        IDP_SOURCE,
        () ->
            MockIdp.attach().recordedRequests().stream()
                .map(
                    request ->
                        NetworkEdge.http(
                            StoryTarget.SERVICE,
                            MockIdp.SERVICE_NAME,
                            request.method() + " " + request.path() + " -> " + request.status()))
                .toList());
    StoryPeers.install();
  }
}
