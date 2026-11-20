package eu.wohlben.qits.orchestrator.api.cleanup;

import java.time.Instant;
import java.util.List;

/**
 * Every repository's collection figures — {@code POST /artifacts/api/gc/repositories}'s answer,
 * handed on by {@code listArtifactsCleanupRepositories}.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcRepositoriesPlanResponse}
 * field for field. Schema only: see the package documentation.
 */
public record GcRepositoriesPlanResponse(
    Instant generatedAt,
    boolean executable,
    List<String> pinFailures,
    String graceWindow,
    List<GcRepositoryPlanSummary> repositories) {}
