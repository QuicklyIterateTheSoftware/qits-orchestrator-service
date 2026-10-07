package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.control.Runs;
import eu.wohlben.qits.orchestrator.dto.RunDetailDto;
import eu.wohlben.qits.orchestrator.error.NoSuchRunException;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * One run, with its steps — the page the UI polls.
 *
 * <p>Its own root rather than a child of {@code /processes/{kind}/runs/{id}}: a run id is unique on
 * its own, and a run's address is a thing an operator pastes into a message. Two paths for one row
 * would mean two links for one run.
 */
@Path("/runs")
@Produces(MediaType.APPLICATION_JSON)
public class RunController {

  @Inject Runs runs;

  /**
   * A run and every step of it: status, timings, the request that went out, the peer's answer
   * whole, the error and the one-line summary.
   *
   * <p>An id that is not a uuid is a 404 like any other unknown run — a malformed id and an absent
   * one are the same question from the caller's side.
   */
  @GET
  @Path("/{id}")
  @Operation(summary = "One run with its steps")
  @APIResponse(responseCode = "200", description = "The run")
  @APIResponse(responseCode = "404", description = "No such run")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:system", "qits:agent"})
  public RunDetailDto run(@PathParam("id") String id) {
    UUID runId;
    try {
      runId = UUID.fromString(id);
    } catch (IllegalArgumentException e) {
      throw new NoSuchRunException(id);
    }
    return runs.run(runId);
  }
}
