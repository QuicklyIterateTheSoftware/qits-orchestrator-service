package eu.wohlben.qits.orchestrator.bus;

import eu.wohlben.qits.eventstream.QitsEvent;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.orchestrator.run.RunAnnouncer;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * <b>The {@link RunAnnouncer} adapter</b> (qits-2472): each run transition becomes a {@link
 * ProcessRunChanged} on the bus, each step transition a {@link ProcessRunStepChanged}. The executor
 * tells it after the store write returned, never inside it, and catches anything it throws.
 *
 * <p>{@code @DefaultBean}, as qits-projects' {@code IdeaEventAnnouncer} is, so a recording test
 * double wins the injection simply by existing. {@code QitsEventBus.publish} never throws — an
 * event it cannot deliver inline goes to its outbox — and dark ({@code
 * qits.eventstream.enabled=false}, {@code %dev} / {@code %test}) it is a DEBUG line and nothing
 * else.
 *
 * <p><b>The publish is synchronous on the caller's thread</b>: the opening event on whatever
 * started the run (the route, the cron, the deploy trigger), every other on the executor's worker.
 * The bus bounds it with its own publish timeout, and a slow bus is at worst a slower run, never a
 * different one.
 */
@ApplicationScoped
@DefaultBean
public class RunEventAnnouncer implements RunAnnouncer {

  @Inject QitsEventBus bus;

  @Override
  public void runChanged(RunChange change) {
    publish(event(change));
  }

  @Override
  public void stepChanged(StepChange change) {
    publish(event(change));
  }

  /** The one place an event leaves; a test subclass overrides it to record instead. */
  protected void publish(QitsEvent event) {
    bus.publish(event);
  }

  /** The wire event of a run's transition: the domain's enums spelled as the API spells them. */
  static ProcessRunChanged event(RunChange change) {
    return new ProcessRunChanged(
        null,
        change.kind(),
        change.runId(),
        change.status().name(),
        change.trigger().wireName(),
        change.dryRun(),
        change.startedAt(),
        change.finishedAt(),
        change.changedAt());
  }

  /** The wire event of a step's transition. */
  static ProcessRunStepChanged event(StepChange change) {
    return new ProcessRunStepChanged(
        null,
        change.kind(),
        change.runId(),
        change.stepId(),
        change.status().name(),
        change.changedAt());
  }
}
