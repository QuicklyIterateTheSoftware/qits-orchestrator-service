package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.api.cleanup.GcRepositoriesPlanResponse;
import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.process.gc.ArtifactsCleanup;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * qits-artifacts' cleanup listing, asked through this service (ticket qits-2138).
 *
 * <p>Served under {@code /orchestrator/api/artifacts/repositories} — {@code /orchestrator/api} is
 * {@code quarkus.rest.path}, as for every resource here.
 *
 * <p><b>Why this door exists.</b> qits-artifacts-frontend's repository listing shows what a
 * collection of each repository would reclaim, judged against the platform's pin set. The services
 * that answer the pins are private and qits-artifacts is public, so it cannot read them; this
 * service can, so the listing asks here. It reads the pins exactly as a gc run does and hands them
 * to qits-artifacts' supplied-pins listing — {@link ArtifactsCleanup} is the whole of it.
 *
 * <p><b>The per-repository plan and sweep are gone</b> (qits-2477): only the landing app's cleanup
 * page used them, and the daily gc does that work.
 *
 * <p><b>The answer is qits-artifacts' own, status and body verbatim</b>: the text it sent, never
 * re-bound into a type here. Only when qits-artifacts could not be asked at all — unreachable, no
 * bearer, an answer larger than this service will read — is the answer this service's: a 502 with
 * {@code {"message": "..."}}, the platform's envelope. The 200 is documented with qits-artifacts'
 * shape ({@code api.cleanup}), schema only.
 *
 * <p><b>The roles are {@link ProcessController#start}'s</b>, all five. A read deletes nothing and
 * takes no lock.
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
  @APIResponse(
      responseCode = "200",
      description = "qits-artifacts' repository listing",
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema = @Schema(implementation = GcRepositoriesPlanResponse.class)))
  @APIResponse(responseCode = "502", description = "qits-artifacts could not be asked")
  @RolesAllowed({"qits:admin", "qits:admin-agent", "qits:desk-admin", "qits:system", "qits:agent"})
  public Response repositories() {
    return passThrough(cleanup.repositories());
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
