package eu.wohlben.qits.orchestrator.bus;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.control.CanonicalJson;
import eu.wohlben.qits.eventstream.control.EventFrame;
import eu.wohlben.qits.orchestrator.schedule.GcDeployTrigger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * <b>A deployment is live, so what it superseded is collectable.</b> This service's first — and so
 * far only — consumption of the platform's event bus.
 *
 * <p>What it does is one call, {@link GcDeployTrigger#onDeploymentActive}, and every decision behind
 * it (the trailing-edge debounce, the two gates, never a dry run, what a busy executor means) is
 * stated there. This class is the seam: decode, hand over.
 *
 * <p><b>Why a deployment at all.</b> The gc cron is a clock, and a clock is a guess about when there
 * is something to collect. A deployment is the fact — the predecessor container's image and every
 * registry identity the release it carried superseded stop being referenced at exactly that moment —
 * and until this listener existed those bytes waited for 03:00 the next day for no reason but the
 * schedule. The cron stays, unchanged, as the backstop.
 *
 * <h2>The payload is a LOCAL record, and that is a decision rather than a shortcut</h2>
 *
 * <p>qits-deployments publishes a vocabulary jar — {@code qits-platform-deployments-events}, which
 * carries the real {@code DeploymentActive} — and this service does not depend on it, for the reason
 * this repository's first rule gives: a clone builds and tests green on its own, and a jar the
 * platform's Maven registry does not serve is a build that resolves out of somebody's {@code ~/.m2}
 * and fails in a release pipeline's step container. That registry serves nothing under that
 * coordinate (measured 2026-09-03 from qits-projects, which reached the same answer). So the payload
 * is bound into a local record by {@link CanonicalJson}, the platform's standing answer for a
 * cross-repo event.
 *
 * <p>The cost is honest and is the cost every cross-repo contract carries: a rename over there is
 * silent here. What makes it survivable is that the wire name is a signature string on both sides
 * and the fields are named in one place, so a change at least has to be a diff.
 *
 * <h2>Failure</h2>
 *
 * <p>The seam's rule — {@link #onFrame} runs inside the transaction that claims the event, so a
 * throw rolls the claim back and the event is owed forever. <b>Nothing here throws.</b> A payload
 * that will not read is poison: the same bytes would fail identically on every later offer, so it is
 * a WARN and a return, which settles it. And the handover itself cannot fail — arming a timer is
 * memory and a schedule, and the run it eventually starts runs on the timer's own thread, well
 * outside this transaction. A gc run must never be inside the claim of the event that asked for it:
 * it is minutes of other people's deleting, and it would hold both this transaction and this
 * consumer's watermark for the whole of it.
 *
 * <h2>Where it starts reading</h2>
 *
 * <p>{@link #replayFromEpoch()} is left at its default, which is the head of the log, and it is a
 * choice rather than an omission: a brand-new consumer replaying from the epoch would walk every
 * deployment this platform has ever made and arm a collection for each one — hundreds of events,
 * every one of them long since superseded by the next, collapsing into one run that would have
 * happened tonight anyway. "From now on, a deployment causes a collection" is exactly the semantics
 * wanted.
 */
@ApplicationScoped
public class DeploymentActiveListener implements QitsDurableEventListener {

  private static final Logger LOG = Logger.getLogger(DeploymentActiveListener.class);

  /** qits-deployments' "this is what serves now" — {@code DeploymentActive}, as the wire spells it. */
  static final String SIGNATURE = "DeploymentActive";

  /**
   * This consumer's storage key, in {@code consumed_event} and {@code consumer_watermark}.
   * <b>Never change it</b> — a new value is a brand-new consumer initializing at the head of the
   * log, silently skipping every deployment in between. It names the consumption, not the class.
   */
  static final String CONSUMER_ID = "orchestrator-gc-on-deploy";

  /**
   * The fields this listener reads, as a local record bound by {@link CanonicalJson}. Unknown fields
   * are ignored by the library's mapper, which is what lets qits-deployments keep sending the ones
   * this consumer has no use for — {@code deploymentId}, {@code environmentId}, {@code commitSha},
   * {@code containerName}, {@code endpoints}, {@code navigation} and the rest all travel.
   *
   * <p><b>All three are the log line and nothing more.</b> This trigger does not care WHICH
   * application deployed: the keep-set the run reads is recomputed from all five pin sources, so no
   * decision here is a function of the payload. They are bound rather than dropped so that a run in
   * the history can be traced back to the wave that caused it, and so that a payload which is not a
   * {@code DeploymentActive} at all fails to bind instead of being acted on.
   *
   * <p>{@code eventId} and {@code occurredAt} are absent on purpose: the first is {@code QitsEvent}'s
   * own accessor, which the canonical mix-in keeps out of every payload, and the second is read off
   * the frame where it is ever wanted.
   *
   * <p>Public so {@link EventWireReflection} and the wire test can name it.
   */
  public record DeploymentActivePayload(
      String applicationName, String version, String environmentName) {}

  @Inject GcDeployTrigger trigger;

  @Override
  public String consumerId() {
    return CONSUMER_ID;
  }

  @Override
  public Set<String> signatures() {
    return Set.of(SIGNATURE);
  }

  @Override
  public void onFrame(EventFrame frame) {
    DeploymentActivePayload deployment = decode(frame);
    if (deployment == null) {
      // Warned in decode. Returning settles the event: the same bytes would fail identically on
      // every later offer, and an event nothing can read must not hold the watermark.
      return;
    }
    // A deployment carrying no version is still a deployment — it superseded a container and its
    // image whatever it called itself — so, unlike qits-projects' consumer of the same event, there
    // is nothing here to refuse. The value is a log line.
    trigger.onDeploymentActive(
        deployment.applicationName(), deployment.version(), deployment.environmentName());
  }

  /** Null on anything that will not read as this payload, warned about once, never thrown. */
  private DeploymentActivePayload decode(EventFrame frame) {
    try {
      return CanonicalJson.payloadTo(frame.payload(), DeploymentActivePayload.class);
    } catch (RuntimeException e) {
      LOG.warnf("%s %s has an unreadable payload: %s", frame.name(), frame.id(), e.getMessage());
      return null;
    }
  }
}
