package eu.wohlben.qits.orchestrator.api.cleanup;

import java.util.List;

/**
 * The blob unlinks a sweep made, and every candidate it did not.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcSweepOutcome} field for
 * field. Schema only: see the package documentation.
 */
public record GcSweepOutcome(
    int blobsUnlinked,
    long bytesReclaimed,
    int withheldByGraceWindow,
    long withheldBytes,
    int stillReferenced,
    int alreadyGone,
    List<String> unlinkedBlobIds) {}
