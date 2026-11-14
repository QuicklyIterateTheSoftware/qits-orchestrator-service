package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.process.gc.ArtifactsCleanup;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * qits-artifacts' per-repository cleanup, asked through this service (ticket qits-2138).
 *
 * <p>Served under {@code /orchestrator/api/artifacts/repositories} — {@code /orchestrator/api} is
 * {@code quarkus.rest.path}, as for every resource here.
 *
 * <p><b>Why these doors exist.</b> qits-artifacts' cleanup UI judges every repository against the
 * platform's pin set, and the five services that answer it are private now while qits-artifacts is
 * public: it cannot read them any more. This service is private, holds the credential all five
 * accept and already reads them for every gc run, so the UI asks here instead. Each door reads the
 * pins exactly as a gc run does and hands them to qits-artifacts' supplied-pins door of the same
 * name — {@link ArtifactsCleanup} is the whole of it, and this class is only the HTTP edge of it.
 *
 * <p><b>The answer is qits-artifacts' own, status and body verbatim</b>: the text it sent, never
 * re-bound into a type here, so its shapes stay its contract and a 404 for an unknown repository
 * stays a 404. Only when qits-artifacts could not be asked at all — unreachable, no bearer, an
 * answer larger than this service will read — is the answer this service's: a 502 with {@code
 * {"message": "..."}}, the platform's envelope.
 *
 * <p><b>The roles are {@link ProcessController#start}'s</b>, all five. A sweep deletes registry
 * content, which is what starting a gc run already lets each of them do across every repository at
 * once; one repository is less, not more. The sweep is refused with a 409 while a gc run is active,
 * and a gc start with a 409 while a sweep is in flight ({@code GcExclusion}). The two reads take no
 * lock: a plan deletes nothing.
 */
@Path("/artifacts/repositories")
@Produces(MediaType.APPLICATION_JSON)
public class ArtifactsCleanupController {

  @Inject ArtifactsCleanup cleanup;

  @GET
  @Operation(
      operationId = "listArtifactsCleanupRepositories",
      summary = "Every artifacts repository, with what a collection would reclaim",
      description =
          "Reads the platform's pins and hands them to qits-artifacts' POST"
              + " /artifacts/api/gc/repositories; its answer is returned verbatim.")
  @APIResponse(responseCode = "200", description = "qits-artifacts' repository listing")
  @APIResponse(responseCode = "502", description = "qits-artifacts could not be asked")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public Response repositories() {
    return passThrough(cleanup.repositories());
  }

  @GET
  @Path("/{repository}/plan")
  @Operation(
      operationId = "planArtifactsCleanupRepository",
      summary = "One artifacts repository's collection plan; nothing is deleted",
      description =
          "Reads the platform's pins and hands them to qits-artifacts' POST"
              + " /artifacts/api/gc/repositories/{repository}/plan; its answer is returned"
              + " verbatim.")
  @APIResponse(responseCode = "200", description = "qits-artifacts' plan for the repository")
  @APIResponse(responseCode = "404", description = "qits-artifacts has no such repository")
  @APIResponse(responseCode = "502", description = "qits-artifacts could not be asked")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public Response plan(@PathParam("repository") String repository) {
    return passThrough(cleanup.plan(repository));
  }

  @POST
  @Path("/{repository}/sweep")
  @Operation(
      operationId = "sweepArtifactsCleanupRepository",
      summary = "Sweep one artifacts repository",
      description =
          "Reads the platform's pins and hands them to qits-artifacts' POST"
              + " /artifacts/api/gc/repositories/{repository}/sweep; its receipt is returned"
              + " verbatim. Refused while a gc run is active, and no gc run starts while it is in"
              + " flight.")
  @APIResponse(responseCode = "200", description = "qits-artifacts' sweep receipt")
  @APIResponse(responseCode = "404", description = "qits-artifacts has no such repository")
  @APIResponse(responseCode = "409", description = "A gc run is active")
  @APIResponse(responseCode = "502", description = "qits-artifacts could not be asked")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public Response sweep(@PathParam("repository") String repository, @Context SecurityContext caller) {
    Principal principal = caller == null ? null : caller.getUserPrincipal();
    return passThrough(
        cleanup.sweep(repository, principal == null ? null : principal.getName()));
  }

  /**
   * qits-artifacts' status and body as it sent them, or a 502 saying why it could not be asked.
   *
   * <p>The body is the TEXT it sent. The one exception is an answer past the 1 MiB the client keeps
   * as its stored copy: that copy is cut and marked, which is no JSON document, so the parsed tree
   * — the whole answer — is written instead. An answer past the parse limit has no tree at all and
   * is a 502.
   */
  static Response passThrough(PeerExchange exchange) {
    PeerAnswer answer = exchange.answer();
    if (answer.error() != null || answer.httpStatus() == null) {
      return badGateway(
          "qits-artifacts could not be asked: "
              + (answer.error() == null ? exchange.call().url() + " gave no answer" : answer.error()));
    }
    if (answer.httpStatus() == 401 || answer.httpStatus() == 403) {
      // qits-artifacts refusing THIS service's own bearer is this service's fault, not the
      // caller's: handed on verbatim, a browser would read it as its own session failing.
      return badGateway(
          exchange.call().url()
              + " refused qits-orchestrator's own credential with "
              + answer.httpStatus());
    }
    if (answer.truncated()) {
      return badGateway(
          exchange.call().url()
              + " answered more than "
              + PeerClient.PARSE_LIMIT_BYTES
              + " bytes, which this service does not hand on");
    }
    String text =
        PeerClient.bounded(answer.body())
            ? (answer.json() == null ? null : answer.json().toString())
            : answer.body();
    Response.ResponseBuilder response = Response.status(answer.httpStatus());
    if (text != null && !text.isEmpty()) {
      response
          .entity(text)
          .type(answer.json() == null ? MediaType.TEXT_PLAIN_TYPE : MediaType.APPLICATION_JSON_TYPE);
    }
    return response.build();
  }

  private static Response badGateway(String message) {
    return Response.status(Response.Status.BAD_GATEWAY)
        .entity(Map.of("message", message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
