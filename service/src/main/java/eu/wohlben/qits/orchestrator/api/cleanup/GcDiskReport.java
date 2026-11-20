package eu.wohlben.qits.orchestrator.api.cleanup;

/**
 * What the blob store's compaction gave back to the volume after a sweep's unlinks; null when the
 * compaction failed.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcDiskReport} field for field.
 * Schema only: see the package documentation.
 */
public record GcDiskReport(
    String state,
    long blobTablesBytesBefore,
    long blobTablesBytesAfter,
    long databaseBytesBefore,
    long databaseBytesAfter,
    int segmentsDrained,
    int segmentsDropped,
    int segmentsHeld,
    long bytesMoved) {}
