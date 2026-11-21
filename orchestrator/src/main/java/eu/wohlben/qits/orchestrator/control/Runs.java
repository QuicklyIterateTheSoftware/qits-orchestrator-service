package eu.wohlben.qits.orchestrator.control;

import eu.wohlben.qits.orchestrator.dto.ProcessDto;
import eu.wohlben.qits.orchestrator.dto.RequestDto;
import eu.wohlben.qits.orchestrator.dto.RunDetailDto;
import eu.wohlben.qits.orchestrator.dto.RunStepDto;
import eu.wohlben.qits.orchestrator.dto.RunSummaryDto;
import eu.wohlben.qits.orchestrator.dto.StepDto;
import eu.wohlben.qits.orchestrator.entity.OpRun;
import eu.wohlben.qits.orchestrator.entity.OpStep;
import eu.wohlben.qits.orchestrator.error.NoSuchProcessException;
import eu.wohlben.qits.orchestrator.error.NoSuchRunException;
import eu.wohlben.qits.orchestrator.error.NoSuchStepException;
import eu.wohlben.qits.orchestrator.persistence.RunStore;
import eu.wohlben.qits.orchestrator.process.ProcessRegistry;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * The read side: process definitions and run records, in the shapes the API serves.
 *
 * <p>It sits in the domain jar because the shapes are the context's, not the web layer's — {@code
 * service}'s controllers do routing, roles and status codes and nothing else.
 */
@ApplicationScoped
public class Runs {

  @Inject ProcessRegistry registry;

  @Inject RunStore store;

  /** Every registered process, with its steps. */
  public List<ProcessDto> processes() {
    return registry.all().stream().map(Runs::describe).toList();
  }

  /**
   * The newest runs of one kind.
   *
   * <p><b>An unknown kind is a 404, not an empty list.</b> "this process has never run" and "there
   * is no such process" are different answers, and a client that cannot tell them apart shows an
   * empty page for a typo.
   */
  public List<RunSummaryDto> runs(String kind, int limit) {
    registry.byKind(kind).orElseThrow(() -> new NoSuchProcessException(kind));
    return store.runs(kind, limit).stream().map(Runs::summarise).toList();
  }

  /**
   * One run with its steps.
   *
   * <p><b>{@code bodies=false} drops each step's {@code request.body} and {@code response}</b>
   * (ticket qits-2359) and keeps everything else, the request's method and url and the {@code
   * responseBytes} size among it. A RUNNING run is polled every two seconds, and a gc run's stored
   * answers reach a megabyte a step; the size is what lets a client fetch one step's whole body
   * ({@link #step}) only when it is asked to show it.
   */
  public RunDetailDto run(UUID id, boolean bodies) {
    OpRun run = store.run(id).orElseThrow(() -> new NoSuchRunException(id));
    List<RunStepDto> steps =
        store.steps(id).stream().map(step -> describe(step, bodies)).toList();
    return new RunDetailDto(
        run.id,
        run.kind,
        run.trigger,
        run.dryRun,
        run.status,
        run.startedAt,
        run.finishedAt,
        run.summary,
        steps);
  }

  /**
   * One step of one run, bodies included — the {@link RunStepDto} {@link #run} serves for it.
   * {@code stepId} is the definition's step id ({@code artifacts.plan}), the row's {@code id} on
   * the wire. An unknown run and a step the run does not hold are both a 404.
   */
  public RunStepDto step(UUID id, String stepId) {
    store.run(id).orElseThrow(() -> new NoSuchRunException(id));
    return store.steps(id).stream()
        .filter(step -> step.stepId.equals(stepId))
        .findFirst()
        .map(step -> describe(step, true))
        .orElseThrow(() -> new NoSuchStepException(id, stepId));
  }

  private static ProcessDto describe(TechnicalProcess process) {
    return new ProcessDto(
        process.kind(),
        process.name(),
        process.description(),
        process.steps().stream()
            .map(step -> new StepDto(step.id(), step.name(), step.target(), step.dependsOn()))
            .toList());
  }

  private static RunSummaryDto summarise(OpRun run) {
    return new RunSummaryDto(
        run.id,
        run.kind,
        run.trigger,
        run.dryRun,
        run.status,
        run.startedAt,
        run.finishedAt,
        run.summary);
  }

  private static RunStepDto describe(OpStep step, boolean bodies) {
    RequestDto request =
        step.requestUrl == null
            ? null
            : new RequestDto(step.requestMethod, step.requestUrl, bodies ? step.requestBody : null);
    return new RunStepDto(
        step.stepId,
        step.name,
        step.target,
        dependsOn(step.dependsOn),
        step.status,
        step.startedAt,
        step.finishedAt,
        step.httpStatus,
        request,
        bodies ? step.responseBody : null,
        step.responseBody == null
            ? null
            : step.responseBody.getBytes(StandardCharsets.UTF_8).length,
        step.error,
        step.summary);
  }

  /** The stored comma-separated edges, back as a list. Empty rather than null: the UI iterates it. */
  private static List<String> dependsOn(String stored) {
    if (stored == null || stored.isBlank()) {
      return List.of();
    }
    return Arrays.stream(stored.split(",")).map(String::trim).filter(value -> !value.isEmpty()).toList();
  }
}
