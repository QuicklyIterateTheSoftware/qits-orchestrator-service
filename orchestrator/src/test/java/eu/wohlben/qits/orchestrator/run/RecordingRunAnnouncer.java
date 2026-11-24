package eu.wohlben.qits.orchestrator.run;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Records every run and step transition announced (qits-2472), as qits-projects'
 * {@code RecordingIdeaAnnouncer} does — the only {@link RunAnnouncer} in this module, so it wins the
 * {@code Instance} simply by existing, and every class in the suite announces into it harmlessly.
 *
 * <p><b>Switchable to throw.</b> {@link #failing} makes every call record and then throw, which is
 * how {@code RunExecutorTest} proves an announcer that blows up cannot keep a run from closing.
 * Record-then-throw, so the test can still wait on the calls it caused.
 *
 * <p>Read through METHODS only: this is a normal-scoped bean, the test injects a client proxy, and a
 * proxy does not forward field access (AGENTS.md, "Tests").
 */
@ApplicationScoped
public class RecordingRunAnnouncer implements RunAnnouncer {

  /** One call, in the order the executor made it: exactly one of the two fields is set. */
  public record Announced(RunChange run, StepChange step) {

    public UUID runId() {
      return run != null ? run.runId() : step.runId();
    }
  }

  private final List<Announced> announced = new ArrayList<>();

  private volatile boolean failing;

  @Override
  public void runChanged(RunChange change) {
    record(new Announced(change, null));
  }

  @Override
  public void stepChanged(StepChange change) {
    record(new Announced(null, change));
  }

  private void record(Announced call) {
    synchronized (announced) {
      announced.add(call);
    }
    if (failing) {
      throw new IllegalStateException("the announcer blew up");
    }
  }

  /** Every call for {@code runId}, in order. */
  public List<Announced> of(UUID runId) {
    synchronized (announced) {
      return announced.stream().filter(call -> call.runId().equals(runId)).toList();
    }
  }

  /** Whether every call from now on throws after recording. */
  public void failing(boolean failing) {
    this.failing = failing;
  }

  public void clear() {
    synchronized (announced) {
      announced.clear();
    }
    failing = false;
  }
}
