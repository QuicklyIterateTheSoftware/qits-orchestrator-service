package eu.wohlben.qits.orchestrator.schedule;

import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.process.scan.ScanConfig;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The clock of the scan process (ticket qits-2490) — the nightly manifest scans qits-maintenance
 * used to start itself, at the INTERNAL scan's old 00:30.
 *
 * <p>{@link GcSchedule}'s shape: one gate, {@code qits.orchestrator.scan.enabled}; {@code SKIP} on a
 * concurrent execution and the store's active-run check behind it; a run that is still going is a
 * line, not an error. Always a real run — the process has no dry run.
 */
@ApplicationScoped
public class ScanSchedule {

  private static final Logger LOG = Logger.getLogger(ScanSchedule.class);

  @Inject ScanConfig config;

  @Inject RunExecutor executor;

  @Scheduled(
      cron = "{qits.orchestrator.scan.cron}",
      timeZone = "{qits.orchestrator.scan.time-zone}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void onSchedule() {
    if (!config.enabled()) {
      LOG.info(
          "The scan process is disabled (qits.orchestrator.scan.enabled=false); nothing started.");
      return;
    }
    try {
      UUID id = executor.start(ScanProcess.KIND, RunTrigger.SCHEDULED, false);
      LOG.infof("Started the scheduled scan run %s.", id);
    } catch (RunAlreadyActiveException active) {
      LOG.infof("A scan run is still active (%s); this schedule is skipped.", active.activeRunId());
    } catch (RuntimeException e) {
      LOG.error("The scheduled scan run could not be started; the next schedule retries.", e);
    }
  }
}
