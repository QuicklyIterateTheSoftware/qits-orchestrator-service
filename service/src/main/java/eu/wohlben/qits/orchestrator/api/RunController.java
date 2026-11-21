package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.control.Runs;
import eu.wohlben.qits.orchestrator.dto.RunDetailDto;
import eu.wohlben.qits.orchestrator.dto.RunStepDto;
import eu.wohlben.qits.orchestrator.error.NoSuchRunException;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * One run, with its steps — the page the UI polls.
 *
 * <p>Its own root rather than a child of {@code /processes/{kind}/runs/{id}}: a run id is unique on
 * its own, and a run's address is a thing an operator pastes into a message. Two paths for one row
 * would mean two links for one run.
 *
 * <p><b>Two doors</b> (ticket qits-2359): {@code GET /runs/{id}} ({@code getProcessRun}), the run
 * and every step, optionally without the bodies; and {@code GET /runs/{id}/steps/{stepId}} ({@code
 * getProcessRunStep}), one step whole. A client polling a RUNNING run asks for it with {@code
 * bodies=false} and fetches a step's request and response only when somebody opens that step. Both
 * take {@link ProcessController}'s five roles.
 */
@Path("/runs")
@Produces(MediaType.APPLICATION_JSON)
public class RunController {

  @Inject Runs runs;

  /**
   * A run and every step of it: status, timings, the request that went out, the peer's answer
   * whole, the error and the one-line summary.
   *
   * <p><b>{@code bodies=false}</b> serves every step with {@code request.body} and {@code response}
   * null and everything else as it is — the request's method and url, and {@code responseBytes},
   * the size of what {@code GET /runs/{id}/steps/{stepId}} would bring back. The default is {@code
   * true}, the answer this door always gave.
   *
   * <p>An id that is not a uuid is a 404 like any other unknown run — a malformed id and an absent
   * one are the same question from the caller's side.
   */
  @GET
  @Path("/{id}")
  @Operation(operationId = "getProcessRun", summary = "One run with its steps")
  @APIResponse(responseCode = "200", description = "The run")
  @APIResponse(responseCode = "404", description = "No such run")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public RunDetailDto run(
      @PathParam("id") String id,
      @Parameter(
              description =
                  "false to leave out every step's request body and response; responseBytes stays")
          @QueryParam("bodies")
          @DefaultValue("true")
          boolean bodies) {
    return runs.run(runId(id), bodies);
  }

  /**
   * One step of a run, whole: what {@code GET /runs/{id}} serves for it with the bodies, the
   * request body and the stored response included.
   *
   * <p>{@code stepId} is the definition's step id ({@code artifacts.plan}), the step's {@code id}
   * on the wire. An unknown run, an id that is not a uuid and a step the run does not hold are each
   * a 404.
   */
  @GET
  @Path("/{id}/steps/{stepId}")
  @Operation(operationId = "getProcessRunStep", summary = "One step of a run, with its bodies")
  @APIResponse(responseCode = "200", description = "The step")
  @APIResponse(responseCode = "404", description = "No such run, or no such step in it")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public RunStepDto step(@PathParam("id") String id, @PathParam("stepId") String stepId) {
    return runs.step(runId(id), stepId);
  }

  private static UUID runId(String id) {
    try {
      return UUID.fromString(id);
    } catch (IllegalArgumentException e) {
      throw new NoSuchRunException(id);
    }
  }
}
