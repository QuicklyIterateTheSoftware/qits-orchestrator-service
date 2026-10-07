package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.control.Runs;
import eu.wohlben.qits.orchestrator.dto.ProcessDto;
import eu.wohlben.qits.orchestrator.dto.RunSummaryDto;
import eu.wohlben.qits.orchestrator.run.RunExecutor;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Technical processes, and the runs of one.
 *
 * <p>Served under {@code /orchestrator/api/processes} — the {@code /orchestrator/api} prefix is
 * {@code quarkus.rest.path}, not spelled here, so this class carries only its own noun.
 *
 * <p><b>Every route accepts the same pair of roles</b>, {@code qits:admin} (a person, through the
 * gateway's forward-auth headers) and {@code qits:system} (a machine, through a bearer validated
 * against qits-idp). A run is started by an operator in a browser and could as well be
 * started by a machine; a machine-only guard would lock the operator out of the button this service
 * exists to offer. There is no anonymous route here. The reads also take {@code qits:agent} (a
 * commissioned agent); starting a run takes it too, but only of the {@code gc} process — {@code gc}
 * is deleting whatever the process deems garbage, which an owner has decided an agent may trigger
 * (dry-run or real) with no more guard than that.
 */
@Path("/processes")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ProcessController {

  /**
   * The run listing's page size, and its ceiling. A run page shows a handful; a client asking for
   * ten thousand would be asking this service to serialise every response body it has ever stored.
   */
  static final int DEFAULT_LIMIT = 20;

  static final int MAX_LIMIT = 200;

  @Inject Runs runs;

  @Inject RunExecutor executor;

  /** The request body of a start: one flag. */
  public record StartRunRequest(boolean dryRun) {

    /** What a 202 answers with — the id to poll. */
    public record Response(UUID id) {}
  }

  @GET
  @Operation(summary = "Every technical process, with its steps and their dependencies")
  @APIResponse(responseCode = "200", description = "The processes")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:system", "qits:agent"})
  public List<ProcessDto> processes() {
    return runs.processes();
  }

  /**
   * One process's runs, newest first.
   *
   * <p>An unknown kind is a 404 rather than an empty list: "this has never run" and "there is no
   * such process" are different answers.
   */
  @GET
  @Path("/{kind}/runs")
  @Operation(summary = "One process's runs, newest first")
  @APIResponse(responseCode = "200", description = "The runs")
  @APIResponse(responseCode = "404", description = "No process of that kind")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:system", "qits:agent"})
  public List<RunSummaryDto> runs(
      @PathParam("kind") String kind,
      @QueryParam("limit") @DefaultValue("" + DEFAULT_LIMIT) int limit) {
    return runs.runs(kind, Math.clamp(limit, 1, MAX_LIMIT));
  }

  /**
   * Starts a run, and does NOT wait for it.
   *
   * <p><b>202 with the id.</b> A gc run is minutes of somebody else's pruning; an HTTP request is
   * the wrong place to hold that, and a client that got a 200 at the end would have no way to watch
   * it happen. The id is what {@code GET /runs/{id}} takes, and the UI polls it every two seconds
   * while the run is RUNNING.
   *
   * <p><b>409 while a run of that kind is active.</b> Two runs overlapping would compute two pin
   * sets at two moments and hand each to a deleter the other is also driving.
   *
   * @param request {@code {"dryRun": true}} to plan without deleting. A missing body is a real run —
   *     the same default the scheduled trigger has.
   */
  @POST
  @Path("/{kind}/runs")
  @Operation(summary = "Start a run of this process")
  @APIResponse(responseCode = "202", description = "Started; poll GET /runs/{id}")
  @APIResponse(responseCode = "404", description = "No process of that kind")
  @APIResponse(responseCode = "409", description = "A run of that kind is already active")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:system", "qits:agent"})
  public Response start(@PathParam("kind") String kind, StartRunRequest request) {
    boolean dryRun = request != null && request.dryRun();
    UUID id = executor.start(kind, RunTrigger.MANUAL, dryRun);
    return Response.status(Response.Status.ACCEPTED)
        .entity(new StartRunRequest.Response(id))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
