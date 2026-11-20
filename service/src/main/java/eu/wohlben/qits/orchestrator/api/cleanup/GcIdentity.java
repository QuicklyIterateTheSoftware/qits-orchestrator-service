package eu.wohlben.qits.orchestrator.api.cleanup;

/**
 * One identity a plan condemns or keeps, or a sweep deleted or withheld, naming the rule that
 * decided it.
 *
 * <p>Mirrors qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto.GcIdentity} field for field.
 * Schema only: see the package documentation.
 */
public record GcIdentity(
    String repository,
    String identity,
    String rule) {}
