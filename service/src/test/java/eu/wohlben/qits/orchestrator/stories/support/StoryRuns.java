package eu.wohlben.qits.orchestrator.stories.support;

import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Starting a run and watching it finish — the two-request dance every run story performs, and the
 * shape the client itself has.
 *
 * <p><b>Why it is two requests and not one.</b> {@code POST /processes/{kind}/runs} answers 202 with
 * the id as soon as the run row and its eighteen PENDING step rows exist; the work is minutes of
 * somebody else's pruning on a single-threaded worker, and an HTTP request is the wrong place to
 * hold it. So a caller polls {@code GET /runs/{id}} — the Angular client does it every two seconds
 * while the run is RUNNING, and these stories do it faster because the peers are next door.
 *
 * <p><b>Every poll is tapped and they all draw as ONE arrow.</b> The label is {@code GET
 * /orchestrator/api/runs/{id} -> 200} — the run id is a uuid and {@link
 * eu.wohlben.qits.userflows.Labels} rewrites it — so twenty polls of one run and one poll of
 * another are the same edge. That is the right reading: what the diagram says is that an operator
 * watches a run over this route, not how impatient the watching was.
 *
 * <p><b>A story must not return until the run has closed.</b> The peers' recording is drained at
 * story end, and a run still in flight would put its remaining calls in the NEXT story's diagram.
 * {@link #awaitClosed} is what makes that impossible: the executor writes the run's {@code
 * finished_at} after the last step row, and the stub records a line before it answers, so a run
 * that has closed has every line it produced already on disk.
 */
public final class StoryRuns {

  /**
   * Long enough for the sixteen calls an eighteen-step run makes to a stub next door — each store is
   * measured with the same call twice — and short enough to fail rather than hang CI.
   */
  private static final Duration PATIENCE = Duration.ofSeconds(120);

  private StoryRuns() {}

  /**
   * Starts a gc run and returns the id the 202 handed back.
   *
   * <p>The caller is a {@link Supplier} rather than a {@code RequestSpecification} for the same
   * reason {@link #detail} takes one: a spec is single-use, and a story that starts a run and then
   * polls it makes several requests as the same person.
   */
  public static String start(Supplier<RequestSpecification> caller, boolean dryRun) {
    return caller
        .get()
        .contentType(ContentType.JSON)
        .body(StoryTarget.startBody(dryRun))
        .when()
        .post(StoryTarget.GC_RUNS_PATH)
        .then()
        .statusCode(202)
        .contentType(ContentType.JSON)
        .extract()
        .path("id");
  }

  /**
   * Polls until the run is no longer RUNNING and returns the terminal status — the client's own
   * loop, and the story's guarantee that the far side has finished recording.
   */
  public static String awaitClosed(Supplier<RequestSpecification> caller, String id) {
    return detail(caller, id).getString("status");
  }

  /** The same wait, handing back the whole run document so a story can read its steps. */
  public static JsonPath detail(Supplier<RequestSpecification> caller, String id) {
    Instant deadline = Instant.now().plus(PATIENCE);
    while (true) {
      JsonPath run =
          caller
              .get()
              .when()
              .get(StoryTarget.runPath(id))
              .then()
              .statusCode(200)
              .extract()
              .jsonPath();
      if (!"RUNNING".equals(run.getString("status"))) {
        return run;
      }
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("run " + id + " never finished: " + run.prettify());
      }
      sleep();
    }
  }

  /** One step of a run document, by its stable id — {@code steps.find { it.id == '…' }}. */
  public static String stepPath(String stepId) {
    return "steps.find { it.id == '" + stepId + "' }";
  }

  private static void sleep() {
    try {
      Thread.sleep(50);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }
}
