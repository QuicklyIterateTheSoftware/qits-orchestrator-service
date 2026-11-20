package eu.wohlben.qits.orchestrator.api.cleanup;

import java.time.Instant;
import java.util.List;

/**
 * One repository's sweep receipt — {@code POST /artifacts/api/gc/repositories/{repository}/sweep}'s
 * answer, handed on by {@code sweepArtifactsCleanupRepository}.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcRepositorySweepReport} field
 * for field. Schema only: see the package documentation.
 */
public record GcRepositorySweepReport(
    String repository,
    String type,
    Instant executedAt,
    boolean dryRun,
    String graceWindow,
    String aborted,
    List<GcPinSource> pins,
    String strategy,
    String note,
    String error,
    List<GcIdentity> deleted,
    List<GcIdentity> withheldByGraceWindow,
    GcSweepOutcome sweep,
    GcDiskReport disk,
    GcUntouchablePool untouchable,
    String runId) {}
