package eu.wohlben.qits.orchestrator.api.cleanup;

import java.time.Instant;
import java.util.List;

/**
 * One repository's collection plan — {@code POST
 * /artifacts/api/gc/repositories/{repository}/plan}'s answer, handed on by {@code
 * planArtifactsCleanupRepository}. Nothing is deleted.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcRepositoryPlanReport} field
 * for field. Schema only: see the package documentation.
 */
public record GcRepositoryPlanReport(
    String repository,
    String type,
    Instant generatedAt,
    boolean dryRun,
    String graceWindow,
    boolean executable,
    List<String> pinFailures,
    List<GcPinSource> pins,
    GcTypeConfiguration configuration,
    String strategy,
    String note,
    String error,
    List<GcIdentity> dead,
    List<GcIdentity> kept,
    GcSweepPlan sweep,
    GcSweepPlan structural,
    GcUntouchablePool untouchable) {}
