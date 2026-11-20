package eu.wohlben.qits.orchestrator.api.cleanup;

import java.util.List;

/**
 * The row-less blob pool, restated: no plan or sweep of any scope may include it.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcUntouchablePool} field for
 * field. Schema only: see the package documentation.
 */
public record GcUntouchablePool(
    String reason,
    int blobCount,
    long bytes,
    List<String> blobIds) {}
