package eu.wohlben.qits.orchestrator.api.cleanup;

/**
 * One repository's row in {@link GcRepositoriesPlanResponse}.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcRepositoryPlanSummary} field
 * for field. Schema only: see the package documentation.
 */
public record GcRepositoryPlanSummary(
    String repository,
    String type,
    String strategy,
    String note,
    String error,
    int identitiesCondemned,
    int identitiesKept,
    int blobsSweepable,
    long reclaimableBytes,
    int withheldByGraceWindow,
    long withheldBytes) {}
