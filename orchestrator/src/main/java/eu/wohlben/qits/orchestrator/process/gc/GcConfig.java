package eu.wohlben.qits.orchestrator.process.gc;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The gc run's policy: how old is old enough, how much cache to keep, and what is never a
 * candidate.
 *
 * <p><b>Policy here, rules with the owner.</b> Nothing on this class decides WHETHER something may
 * be deleted — that is qits-artifacts' engine and qits-containers' keep-rules, each beside the
 * store it owns. What lives here are the numbers a platform tunes: they travel into a request body
 * and the owner applies them.
 */
@ApplicationScoped
public class GcConfig {

  @ConfigProperty(name = "qits.orchestrator.gc.enabled")
  boolean enabled;

  @ConfigProperty(name = "qits.orchestrator.gc.dry-run")
  boolean scheduledDryRun;

  @ConfigProperty(name = "qits.orchestrator.gc.deploy-trigger.enabled")
  boolean deployTriggerEnabled;

  @ConfigProperty(name = "qits.orchestrator.gc.deploy-trigger.quiet-period")
  Duration deployTriggerQuietPeriod;

  @ConfigProperty(name = "qits.orchestrator.gc.image-keep-prefixes")
  List<String> imageKeepPrefixes;

  @ConfigProperty(name = "qits.orchestrator.gc.branch-keep-prefixes")
  List<String> branchKeepPrefixes;

  @ConfigProperty(name = "qits.orchestrator.gc.image-min-age")
  Duration imageMinAge;

  @ConfigProperty(name = "qits.orchestrator.gc.volume-min-age")
  Duration volumeMinAge;

  @ConfigProperty(name = "qits.orchestrator.gc.build-cache-keep-bytes")
  long buildCacheKeepBytes;

  @ConfigProperty(name = "qits.orchestrator.gc.builder-cache-keep-bytes")
  long builderCacheKeepBytes;

  @ConfigProperty(name = "qits.orchestrator.gc.tags-sweep-call-timeout")
  Duration tagsSweepCallTimeout;

  /** Whether the CLOCK may start a run. A manual run ignores this — a person is the trigger. */
  public boolean enabled() {
    return enabled;
  }

  /** Whether the SCHEDULED run deletes. A manual run carries its own flag from the request. */
  public boolean scheduledDryRun() {
    return scheduledDryRun;
  }

  /**
   * Whether a DEPLOYMENT may start a run — the bus trigger's own gate, beside {@link #enabled()}
   * rather than folded into it, because they answer different questions: {@code gc.enabled} is
   * whether an unattended run may happen at all, and this is whether deployments are one of the
   * things that cause one. Off, the listener still consumes and settles its events; it simply arms
   * nothing, and the cron is the only unattended trigger left.
   */
  public boolean deployTriggerEnabled() {
    return deployTriggerEnabled;
  }

  /**
   * How long after the LAST deployment the triggered run fires — a trailing-edge debounce.
   *
   * <p><b>A deployment wave is one event, not eight.</b> A platform release rolls several
   * applications within a few minutes, and each one supersedes images and registry identities the
   * one before it did not. Firing per event would start a run against a store still being written,
   * seven of them refused as already-active; waiting out a quiet period after the last one collapses
   * the whole wave into ONE run, which then drains everything the wave superseded — registry
   * identities and host images both — in a single pass.
   */
  public Duration deployTriggerQuietPeriod() {
    return deployTriggerQuietPeriod;
  }

  /** Image tag prefixes qits-containers must keep whatever its own rules say. */
  public List<String> imageKeepPrefixes() {
    return List.copyOf(imageKeepPrefixes);
  }

  /**
   * Branch prefixes the merged-branch sweep may never condemn. Additive to qits-workspaces' own
   * hard refusals (the main branch, {@code environment/*}, workspace-backed branches) — this key
   * can widen the protection and never narrow it.
   */
  public List<String> branchKeepPrefixes() {
    return List.copyOf(branchKeepPrefixes);
  }

  /** How young an image is protected from the age rule — the build-then-push window. */
  public Duration imageMinAge() {
    return imageMinAge;
  }

  /** How young a dangling volume is protected — the stop-then-start window. */
  public Duration volumeMinAge() {
    return volumeMinAge;
  }

  /** What the HOST builder may keep after a prune — the cache every CI build warms and re-reads. */
  public long buildCacheKeepBytes() {
    return buildCacheKeepBytes;
  }

  /**
   * What a {@code buildx_buildkit_*} BUILDER container may keep.
   *
   * <p>Its own number because it is a bootstrap-time cache: warmed once while a machine is built,
   * then unread until the next bootstrap. Sharing the host's budget is what left a 13.7 GB builder
   * untouched every night — it was smaller than the budget, so the prune never reached it.
   */
  public long builderCacheKeepBytes() {
    return builderCacheKeepBytes;
  }

  /**
   * How long {@code tags.sweep} alone may take — longer than every other step's {@code
   * call-timeout}, because it is the one call that walks dozens of repositories and pushes
   * decommissioning commits to GitHub rather than reading or writing one store.
   */
  public Duration tagsSweepCallTimeout() {
    return tagsSweepCallTimeout;
  }
}
