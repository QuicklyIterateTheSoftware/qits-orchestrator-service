package eu.wohlben.qits.orchestrator.api.cleanup;

import java.util.List;

/**
 * What a sweep would unlink — with the grace window applied ({@code sweep}) or not ({@code
 * structural}) — and what the window withholds.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcSweepPlan} field for field.
 * Schema only: see the package documentation.
 */
public record GcSweepPlan(
    int blobCount,
    long reclaimableBytes,
    int withheldByGraceWindow,
    long withheldBytes,
    List<String> blobIds) {}
