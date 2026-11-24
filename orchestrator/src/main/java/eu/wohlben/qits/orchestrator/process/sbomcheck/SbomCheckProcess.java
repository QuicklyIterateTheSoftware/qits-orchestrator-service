package eu.wohlben.qits.orchestrator.process.sbomcheck;

import com.fasterxml.jackson.databind.JsonNode;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/**
 * The SBOM check process (ticket qits-2490): qits-maintenance's daily check of every released
 * version still in qits-artifacts for a usable SBOM, run here so it has a run history, a Run now
 * and events — until then it was a cron inside qits-maintenance with a log line for an account.
 *
 * <p><b>One step, {@code check}</b>: {@code POST /maintenance/api/sbom-check/runs}, no body. The
 * door is SYNCHRONOUS — it answers 202 with the report it stored ({@code {ranAt, filed, entries,
 * warnings, tickets}}) once the whole check is done, and 502 when qits-artifacts could not be
 * asked — so the call has a read timeout of its own, {@link SbomCheckConfig#callTimeout()}, rather
 * than the peers' shared one. What it counts and which tickets it files are qits-maintenance's
 * decisions; the summary only reads the report's figures.
 *
 * <p><b>No dry run</b> ({@link #dryRunnable()}): the door has no such mode, and whether it files
 * tickets is qits-maintenance's own {@code file-tickets} setting.
 */
@ApplicationScoped
public class SbomCheckProcess implements TechnicalProcess {

  /** The kind, in every url and in every stored row. */
  public static final String KIND = "sbom-check";

  public static final String CHECK = "check";

  /** qits-maintenance's check door. */
  public static final String RUNS_PATH = "/maintenance/api/sbom-check/runs";

  @Inject SbomCheckConfig config;

  /** The process outside CDI, for a test. */
  public static SbomCheckProcess of(SbomCheckConfig config) {
    SbomCheckProcess process = new SbomCheckProcess();
    process.config = config;
    return process;
  }

  @Override
  public String kind() {
    return KIND;
  }

  @Override
  public String name() {
    return "SBOM check";
  }

  @Override
  public String description() {
    return "Asks qits-maintenance to check every released version still in the registry for a"
        + " usable SBOM, re-read the ones that are missing or failed, and keep one ticket open per"
        + " artifact that still has none. Reports the versions without one, the open tickets and"
        + " any warnings.";
  }

  @Override
  public boolean dryRunnable() {
    return false;
  }

  @Override
  public List<StepDefinition> steps() {
    return List.of(
        new StepDefinition(
            CHECK, "Check the SBOMs", PeerTarget.MAINTENANCE, List.of(), this::check));
  }

  private StepResult check(RunContext context) {
    return StepResult.of(
        context.peers().post(PeerTarget.MAINTENANCE, RUNS_PATH, null, config.callTimeout()),
        answer -> summary(answer.json()));
  }

  /**
   * {@code {filed, entries[], warnings[], tickets[]}}: how many released versions have no usable
   * SBOM, how many tickets are open, how many warnings — and when the run filed nothing because
   * qits-maintenance's ticket filing is off, that it only reported.
   */
  static String summary(JsonNode report) {
    int entries = report.path("entries").size();
    int tickets = report.path("tickets").size();
    int warnings = report.path("warnings").size();
    return entries
        + (entries == 1 ? " version" : " versions")
        + " without a usable SBOM, "
        + tickets
        + (tickets == 1 ? " ticket" : " tickets")
        + " open, "
        + warnings
        + (warnings == 1 ? " warning" : " warnings")
        + (report.path("filed").asBoolean(false) ? "" : " (report only: no tickets filed)");
  }
}
