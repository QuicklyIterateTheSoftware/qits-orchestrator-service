package eu.wohlben.qits.orchestrator.process.sbomcheck;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The SBOM check process's settings (ticket qits-2490): whether the clock may start it, and how
 * long its one synchronous call may take. What the check counts and files is qits-maintenance's.
 */
@ApplicationScoped
public class SbomCheckConfig {

  @ConfigProperty(name = "qits.orchestrator.sbom-check.enabled")
  boolean enabled;

  @ConfigProperty(name = "qits.orchestrator.sbom-check.call-timeout")
  Duration callTimeout;

  /** Whether the CLOCK may start a run. A manual run ignores this — a person is the trigger. */
  public boolean enabled() {
    return enabled;
  }

  /** The read timeout of the check's call: the door answers only once the whole check is done. */
  public Duration callTimeout() {
    return callTimeout;
  }

  /** The settings as given, for a test outside CDI. */
  public static SbomCheckConfig of(boolean enabled, Duration callTimeout) {
    SbomCheckConfig config = new SbomCheckConfig();
    config.enabled = enabled;
    config.callTimeout = callTimeout;
    return config;
  }
}
