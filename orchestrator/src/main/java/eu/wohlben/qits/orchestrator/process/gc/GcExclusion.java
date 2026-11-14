package eu.wohlben.qits.orchestrator.process.gc;

import eu.wohlben.qits.orchestrator.error.RepositorySweepInFlightException;
import eu.wohlben.qits.orchestrator.error.RunAlreadyActiveException;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * <b>A gc run and an artifacts cleanup sweep never overlap</b> (ticket qits-2138): one guard, which
 * both sides go through.
 *
 * <p>Both hand qits-artifacts a pin set read at their own moment and ask it to delete. Overlapping,
 * the second would judge against a photograph of a store the first is emptying — the reason one gc
 * run at a time is a rule ({@link RunAlreadyActiveException}), seen across two doors. So a sweep is
 * refused while a gc run is active, and a gc run — manual, scheduled or deploy-triggered alike, all
 * of which go through {@code RunExecutor.start} — is refused while a sweep is in flight. The two
 * cleanup READS take no part: a plan deletes nothing.
 *
 * <p><b>Check and claim are one step, under one monitor.</b> A sweep checks for an active gc run and
 * registers itself before it releases the monitor; a gc start checks for a registered sweep and
 * OPENS its run row (committed by {@link RunStore#open}) before it releases it. Whichever arrives
 * second sees the first, so a person pressing Sweep while the deploy trigger fires is refused one
 * way or the other rather than raced. The sweep itself — the pin reads and the call — runs outside
 * the monitor, holding only its registration, so a slow sweep blocks no read and no other sweep.
 *
 * <p><b>Why in-process is enough.</b> qits-deployments runs every application as exactly one swarm
 * task ({@code SwarmDeploymentDriver.DEPLOYED_REPLICAS}), and this service already leans on that:
 * {@code RunExecutor} has ONE worker thread for the whole service. A gc run's half of the check is
 * the database row ({@link RunStore#live}), which a second process would see too; the sweep's
 * registration is memory, which it would not. The one moment two processes exist is a rollout's
 * {@code start-first} overlap, while the predecessor drains: a sweep still in flight on the old task
 * is invisible to a gc started on the new one. The deploy trigger fires only after its quiet period,
 * long after the old task is gone, so the window is a person starting a manual run in the seconds of
 * a cutover while another person's sweep is still answering — accepted, and stated here rather than
 * solved with a lock row nothing else needs.
 */
@ApplicationScoped
public class GcExclusion {

  /** The monitor both checks hold. */
  private final Object lock = new Object();

  /** Sweeps in flight, by repository, with how many of each — guarded by {@link #lock}. */
  private final Map<String, Integer> sweeping = new LinkedHashMap<>();

  @Inject RunStore runs;

  /**
   * Opens a run of {@code kind} through {@code open}, refusing a gc run while a sweep is in flight.
   * Any other kind opens unguarded.
   *
   * @throws RepositorySweepInFlightException a sweep is in flight — a 409
   */
  public <T> T opening(String kind, Supplier<T> open) {
    if (!GcProcess.KIND.equals(kind)) {
      return open.get();
    }
    synchronized (lock) {
      if (!sweeping.isEmpty()) {
        throw new RepositorySweepInFlightException(sweeping.keySet().iterator().next());
      }
      return open.get();
    }
  }

  /**
   * Runs one repository sweep, refused while a gc run is active, and registered as in flight for as
   * long as {@code sweep} runs.
   *
   * @throws RunAlreadyActiveException a gc run is active — a 409, naming it
   */
  public <T> T sweeping(String repository, Supplier<T> sweep) {
    synchronized (lock) {
      Optional<UUID> active = runs.live(GcProcess.KIND, Instant.now());
      if (active.isPresent()) {
        throw new RunAlreadyActiveException(GcProcess.KIND, active.get());
      }
      sweeping.merge(repository, 1, Integer::sum);
    }
    try {
      return sweep.get();
    } finally {
      synchronized (lock) {
        sweeping.computeIfPresent(repository, (name, count) -> count == 1 ? null : count - 1);
      }
    }
  }

  /** Whether a sweep is in flight right now. For the tests; nothing decides on it unlocked. */
  public boolean sweepInFlight() {
    synchronized (lock) {
      return !sweeping.isEmpty();
    }
  }
}
