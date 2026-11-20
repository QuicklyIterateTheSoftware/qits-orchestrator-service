package eu.wohlben.qits.orchestrator.api.cleanup;

/**
 * A repository type's configured engine, window and rule, as a sentence.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcTypeConfiguration} field for
 * field. Schema only: see the package documentation.
 */
public record GcTypeConfiguration(
    String type,
    String strategy,
    String window,
    String rule) {}
