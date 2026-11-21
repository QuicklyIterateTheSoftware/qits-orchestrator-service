package eu.wohlben.qits.orchestrator.schedule;

import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.process.gc.GcConfig;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The gc run's second unattended trigger: <b>a deployment went live</b>.
 *
 * <p>The cron beside this class is a clock, and a clock is a guess about when there is something to
 * collect. A deployment is the fact itself — it is the moment the image the predecessor container
 * ran, and every registry identity the release it carried superseded, stop being referenced by
 * anything. Until this existed those bytes sat on the platform's disk until 03:00 the next day for
 * no reason but the schedule. {@code bus/DeploymentActiveListener} is the ear; this is the policy.
 *
 * <h2>Trailing edge, and why not leading</h2>
 *
 * <p>A platform release rolls several applications within a few minutes, so the events arrive as a
 * WAVE. Each new one <b>pushes the pending run back</b> to {@code quiet-period} from now, so the
 * wave collapses into exactly ONE run that fires after the last event — and that run then drains
 * what the whole wave superseded, registry identities and host images together, in a single pass.
 *
 * <p>Firing on the leading edge would do the opposite of what is wanted: the run would plan against
 * a platform still being deployed, the seven deployments behind it would each be refused as
 * already-active, and the bytes the last of them freed would wait for the cron anyway.
 *
 * <h2>What it will not do</h2>
 *
 * <ul>
 *   <li><b>Never a dry run.</b> A dry run is a measurement somebody asked for; this trigger exists
 *       to reclaim, and a debounced dry run would be a nightly report nobody reads.
 *   <li><b>Never past a gate.</b> {@code gc.enabled} and {@code deploy-trigger.enabled} are both
 *       read, and both are read AGAIN when the quiet period lapses rather than only when the wave
 *       started — the run is the thing being gated, not the timer.
 *   <li><b>Never a second concurrent run.</b> {@code RunStore.open} refuses one inside the opening
 *       transaction, which is the platform's real single-flight rule and the reason this class does
 *       not keep a flag of its own. A refusal is the ordinary case for a long run and not a fault,
 *       so it is a DEBUG — and the wave is <b>re-armed</b> for another quiet period, because the
 *       events that caused it were about identities the running run may already have planned
 *       around.
 *   <li><b>Never throw at the caller.</b> {@link #onDeploymentActive} runs on the bus's claim
 *       thread, inside the transaction that claims the event; a throw there rolls the claim back
 *       and the event is owed forever. Everything here is memory and a schedule, and the run itself
 *       starts on the timer's thread, well outside that transaction.
 * </ul>
 */
@ApplicationScoped
public class GcDeployTrigger {

  private static final Logger LOG = Logger.getLogger(GcDeployTrigger.class);

  @Inject GcConfig config;

  @Inject RunExecutor executor;

  @Inject DeployTriggerTimer timer;

  /**
   * Guards {@link #pending}. A deployment wave arrives on the bus's claim thread and lapses on the
   * timer's, so "cancel what is parked and park this instead" has to be one step or two events
   * landing together would leave two runs armed.
   */
  private final Object pendingLock = new Object();

  /** The one parked run, or null when nothing is armed. */
  private DeployTriggerTimer.Pending pending;

  /**
   * One {@code DeploymentActive}, already decoded. Records the wave as still going and moves the
   * pending run to {@code quiet-period} from now.
   *
   * <p>The three fields are the log line and nothing else: this trigger does not care WHICH
   * application deployed, because the keep-set the run reads is recomputed from every pin source
   * anyway. They are here so that a run in the history can be traced back to the wave that caused
   * it.
   */
  public void onDeploymentActive(String applicationName, String version, String environmentName) {
    if (!config.deployTriggerEnabled()) {
      LOG.debugf(
          "%s %s is live in %s; deployments are not a gc trigger here"
              + " (qits.orchestrator.gc.deploy-trigger.enabled=false).",
          applicationName, version, environmentName);
      return;
    }
    if (!config.enabled()) {
      LOG.debugf(
          "%s %s is live in %s; the gc process is disabled (qits.orchestrator.gc.enabled=false).",
          applicationName, version, environmentName);
      return;
    }
    Duration quietPeriod = config.deployTriggerQuietPeriod();
    arm(quietPeriod);
    LOG.debugf(
        "%s %s is live in %s; a gc run is armed for %s from now.",
        applicationName, version, environmentName, quietPeriod);
  }

  /** Parks the run, cancelling whatever the previous event parked. */
  private void arm(Duration delay) {
    synchronized (pendingLock) {
      if (pending != null) {
        pending.cancel();
      }
      pending = timer.after(delay, this::fire);
    }
  }

  /**
   * The quiet period lapsed: start the run.
   *
   * <p>Package-private rather than private because it is the timer's task and the suite drives it
   * through the timer, never directly.
   */
  void fire() {
    synchronized (pendingLock) {
      // This task IS the pending one, so there is nothing left parked — and clearing it before the
      // start means the busy-rearm below parks a new one rather than cancelling itself.
      pending = null;
    }
    if (!config.deployTriggerEnabled() || !config.enabled()) {
      // A gate turned off while the wave was settling. The run is what is gated, so it is asked
      // again here rather than trusted from ten minutes ago.
      LOG.debug("The quiet period lapsed but the gc trigger is disabled now; nothing started.");
      return;
    }
    try {
      // Never dry: this trigger exists to reclaim. SCHEDULED's dry-run key is deliberately not read.
      UUID id = executor.start(GcProcess.KIND, RunTrigger.EVENT, false);
      LOG.infof("Started the deployment-triggered gc run %s.", id);
    } catch (RunAlreadyActiveException active) {
      // The ordinary case, not a fault: the previous run is still going and RunStore refuses a
      // second inside its own opening transaction. Re-arm, so this wave still gets a run of its own
      // rather than being dropped into the one that was already planning when it arrived.
      LOG.debugf(
          "A gc run is still active (%s); this wave's run is re-armed for %s from now.",
          active.activeRunId(), config.deployTriggerQuietPeriod());
      arm(config.deployTriggerQuietPeriod());
    } catch (RuntimeException e) {
      // A store that will not answer, or a bug. A background trigger's failure is a line in the log
      // and nothing else: the next deployment re-arms, and the nightly cron is the backstop.
      LOG.error(
          "The deployment-triggered gc run could not be started;"
              + " the next deployment or the nightly cron retries.",
          e);
    }
  }
}
