package eu.wohlben.qits.orchestrator.process.scan;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The scan process's settings (ticket qits-2490): whether the clock may start it, and how a
 * queued scan is followed to its end. What a scan reads and writes is qits-maintenance's own.
 */
@ApplicationScoped
public class ScanConfig {

  @ConfigProperty(name = "qits.orchestrator.scan.enabled")
  boolean enabled;

  @ConfigProperty(name = "qits.orchestrator.scan.poll-interval")
  Duration pollInterval;

  @ConfigProperty(name = "qits.orchestrator.scan.follow-timeout")
  Duration followTimeout;

  /** Whether the CLOCK may start a run. A manual run ignores this — a person is the trigger. */
  public boolean enabled() {
    return enabled;
  }

  /** How long a follow step waits between two reads of a scan that has not ended yet. */
  public Duration pollInterval() {
    return pollInterval;
  }

  /**
   * How long a follow step keeps reading a scan that is still REQUESTED or RUNNING before it gives
   * up on it — the step FAILS saying so, the scan itself is left to qits-maintenance.
   */
  public Duration followTimeout() {
    return followTimeout;
  }

  /** The settings as given, for a test outside CDI. */
  public static ScanConfig of(boolean enabled, Duration pollInterval, Duration followTimeout) {
    ScanConfig config = new ScanConfig();
    config.enabled = enabled;
    config.pollInterval = pollInterval;
    config.followTimeout = followTimeout;
    return config;
  }
}
