package eu.wohlben.qits.orchestrator.schedule;

import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckConfig;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The clock of the SBOM check process (ticket qits-2490) — the daily check qits-maintenance used to
 * start itself, at its old 02:15.
 *
 * <p>{@link GcSchedule}'s shape: one gate, {@code qits.orchestrator.sbom-check.enabled}; {@code
 * SKIP} on a concurrent execution and the store's active-run check behind it; a run that is still
 * going is a line, not an error. Always a real run — the process has no dry run.
 */
@ApplicationScoped
public class SbomCheckSchedule {

  private static final Logger LOG = Logger.getLogger(SbomCheckSchedule.class);

  @Inject SbomCheckConfig config;

  @Inject RunExecutor executor;

  @Scheduled(
      cron = "{qits.orchestrator.sbom-check.cron}",
      timeZone = "{qits.orchestrator.sbom-check.time-zone}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void onSchedule() {
    if (!config.enabled()) {
      LOG.info(
          "The sbom-check process is disabled (qits.orchestrator.sbom-check.enabled=false);"
              + " nothing started.");
      return;
    }
    try {
      UUID id = executor.start(SbomCheckProcess.KIND, RunTrigger.SCHEDULED, false);
      LOG.infof("Started the scheduled sbom-check run %s.", id);
    } catch (RunAlreadyActiveException active) {
      LOG.infof(
          "An sbom-check run is still active (%s); this schedule is skipped.",
          active.activeRunId());
    } catch (RuntimeException e) {
      LOG.error("The scheduled sbom-check run could not be started; the next schedule retries.", e);
    }
  }
}
