package eu.wohlben.qits.orchestrator.api.cleanup;

import java.time.Instant;
import java.util.List;

/**
 * How one pin source was read for a plan or a sweep: its url, outcome, duration and the
 * keep-identities it produced.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcPinSource} field for field.
 * Schema only: see the package documentation.
 */
public record GcPinSource(
    String source,
    String url,
    boolean answered,
    String outcome,
    Instant readAt,
    long tookMillis,
    int pinCount,
    List<String> keeps) {}
