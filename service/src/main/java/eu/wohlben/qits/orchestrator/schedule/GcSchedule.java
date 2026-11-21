package eu.wohlben.qits.orchestrator.schedule;

import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.process.gc.GcConfig;
import eu.wohlben.qits.orchestrator.process.gc.GcProcess;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The clock — one of the gc run's two unattended triggers, and the BACKSTOP of the pair.
 *
 * <p>{@link GcDeployTrigger} beside it is the other, and it is the one with a reason: a deployment
 * going live is the moment what it superseded stops being referenced, while a clock is only a guess
 * about when that has happened. This schedule stays unchanged for what the guess still catches — a
 * day with no deployment at all, a wave the trigger was down for, and everything that ages out
 * without anything being deployed.
 *
 * <p><b>Two gates, and they say different things.</b> {@code qits.orchestrator.gc.enabled} is
 * whether the clock may start a run at all; {@code qits.orchestrator.gc.dry-run} is whether the run
 * it starts may delete. A platform that wants to watch the figures for a week sets the second and
 * reads the runs in the UI — a dry run still calls every peer and reports real numbers, so it is a
 * measurement rather than a no-op. Neither key touches a MANUAL run: a person choosing Run now is
 * the decision.
 *
 * <p><b>{@code SKIP} on a concurrent execution</b>, the mirror's shape, and the store's active-run
 * check behind it. A run that outlives its own schedule is never joined by a second one — one of
 * the two guards would be enough, and both are here because they fail differently: SKIP costs
 * nothing and leaves no row, while the store's check is what also covers a person and the cron
 * arriving together.
 */
@ApplicationScoped
public class GcSchedule {

  private static final Logger LOG = Logger.getLogger(GcSchedule.class);

  @Inject GcConfig config;

  @Inject RunExecutor executor;

  @Scheduled(
      cron = "{qits.orchestrator.gc.cron}",
      timeZone = "{qits.orchestrator.gc.time-zone}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void onSchedule() {
    if (!config.enabled()) {
      LOG.info("The gc process is disabled (qits.orchestrator.gc.enabled=false); nothing started.");
      return;
    }
    boolean dryRun = config.scheduledDryRun();
    try {
      UUID id = executor.start(GcProcess.KIND, RunTrigger.SCHEDULED, dryRun);
      LOG.infof("Started the scheduled gc run %s (dryRun=%s).", id, dryRun);
    } catch (RunAlreadyActiveException active) {
      // The ordinary case for a long run, not a fault: the last one is still going, and starting a
      // second would plan against a store the first is emptying.
      LOG.infof("A gc run is still active (%s); this schedule is skipped.", active.activeRunId());
    } catch (RuntimeException e) {
      // A background chore's failure is a line in the log and a retry on the next schedule, never a
      // dead scheduler thread or a service that stops answering.
      LOG.error("The scheduled gc run could not be started; the next schedule retries.", e);
    }
  }
}
