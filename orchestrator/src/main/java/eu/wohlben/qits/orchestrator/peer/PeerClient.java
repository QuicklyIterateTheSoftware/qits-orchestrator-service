package eu.wohlben.qits.orchestrator.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The one way a technical process touches another service.
 *
 * <p><b>The JDK's HttpClient, not a REST client</b>, and the deployer's reasons: a peer's answer
 * stays an opaque JSON document rather than a bound record, so nothing here is a second copy of
 * four other repositories' response shapes, and there is no build-time reflection list to keep in
 * step with them.
 *
 * <p><b>Two credentials on every call, and they are not alternatives.</b> {@code X-Qits-User} /
 * {@code X-Qits-Roles} are the forward-auth pair a peer accepts on the platform's own network; the
 * bearer, where a client is enabled, is the machine track. A peer that takes neither refuses the
 * call and the step records the refusal — which is the same shape the deployer's config read has.
 *
 * <p><b>Nothing throws.</b> Every failure — a name that does not resolve, a timeout, a body that is
 * not JSON — comes back as a {@link PeerAnswer} carrying the sentence. A step's job is to record
 * what happened, and a process whose steps had to catch would put half its outcomes on an exception
 * path nobody reads.
 *
 * <p><b>A response is bounded at 1 MiB</b> with a marker appended. An artifacts plan lists every
 * condemned identity on the platform and can be tens of megabytes; the store here is a log a person
 * reads, and an unbounded column would let one peer's verbosity decide this service's disk.
 */
@ApplicationScoped
public class PeerClient {

  /** How much of a peer's answer is kept, and what says so when the rest is dropped. */
  public static final int RESPONSE_LIMIT_BYTES = 1024 * 1024;

  private static final String TRUNCATION_MARKER =
      "\n…truncated by qits-platform-orchestrator at " + RESPONSE_LIMIT_BYTES + " bytes";

  private static final ObjectMapper JSON = new ObjectMapper();

  @ConfigProperty(name = "qits.orchestrator.targets.artifacts-url")
  String artifactsUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.containers-url")
  String containersUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.ci-url")
  String ciUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.deployments-url")
  String deploymentsUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.projects-url")
  String projectsUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.workspaces-url")
  String workspacesUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.maintenance-url")
  String maintenanceUrl;

  @ConfigProperty(name = "qits.orchestrator.targets.configuration-url")
  String configurationUrl;

  @ConfigProperty(name = "qits.orchestrator.gc.call-timeout")
  Duration callTimeout;

  @Inject PeerTokens tokens;

  /** One client for the life of the process — a run is many calls and a new pool per call is waste. */
  private volatile HttpClient client;

  /** The absolute url a step's path resolves to on one peer. */
  public String url(String target, String path) {
    return trimTrailingSlash(base(target)) + path;
  }

  /**
   * A GET, as the pair a step records: what was asked and what came back.
   *
   * <p>{@code get} and {@code post} are the seam a test replaces — a fake peer is an {@code
   * @Alternative} subclass overriding these two, so nothing in a process has to know it is talking
   * to a stub.
   */
  public PeerExchange get(String target, String path) {
    PeerCall call = new PeerCall("GET", url(target, path), null);
    return new PeerExchange(call, send(target, call));
  }

  /** A POST with a JSON body, as the same pair. */
  public PeerExchange post(String target, String path, String body) {
    return post(target, path, body, callTimeout);
  }

  /**
   * A POST with its own read timeout, for the one call this process knows in advance is slower
   * than the rest — qits-projects' tag sweep, which can walk dozens of repositories and push to
   * GitHub. Every other caller goes through the three-argument {@link #post}, which uses {@link
   * #callTimeout}.
   */
  public PeerExchange post(String target, String path, String body, Duration timeout) {
    PeerCall call = new PeerCall("POST", url(target, path), body);
    return new PeerExchange(call, send(target, call, timeout));
  }

  /** Sends one call and turns everything that can happen into an answer. */
  public PeerAnswer send(String target, PeerCall call) {
    return send(target, call, callTimeout);
  }

  /** The same send, with its own read timeout rather than {@link #callTimeout}. */
  public PeerAnswer send(String target, PeerCall call, Duration timeout) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(call.url()))
            .timeout(timeout)
            .header("Accept", "application/json")
            // The forward-auth half: this service's own name and the one role it acts with. Every
            // peer route a process calls is a machine route, so the role is qits:system and never
            // an operator's — a run started by a person is still the orchestrator calling.
            .header("X-Qits-User", "qits-platform-orchestrator")
            .header("X-Qits-Roles", "qits:system");
    if (call.body() == null) {
      request.GET();
    } else {
      request
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(call.body(), StandardCharsets.UTF_8));
    }
    tokens.token(target).ifPresent(token -> request.header("Authorization", "Bearer " + token));

    try {
      HttpResponse<String> response =
          client().send(request.build(), HttpResponse.BodyHandlers.ofString());
      String body = bound(response.body());
      return new PeerAnswer(response.statusCode(), body, parse(body), null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new PeerAnswer(null, null, null, call.url() + " was interrupted");
    } catch (Exception e) {
      return new PeerAnswer(null, null, null, call.url() + " could not be called: " + e);
    }
  }

  /** The peer's base url, from configuration. An unknown target is a programming error. */
  private String base(String target) {
    return switch (target) {
      case PeerTarget.ARTIFACTS -> artifactsUrl;
      case PeerTarget.CONTAINERS -> containersUrl;
      case PeerTarget.CI -> ciUrl;
      case PeerTarget.DEPLOYMENTS -> deploymentsUrl;
      case PeerTarget.PROJECTS -> projectsUrl;
      case PeerTarget.WORKSPACES -> workspacesUrl;
      case PeerTarget.MAINTENANCE -> maintenanceUrl;
      case PeerTarget.CONFIGURATION -> configurationUrl;
      default -> throw new IllegalArgumentException("no such peer: " + target);
    };
  }

  private HttpClient client() {
    HttpClient existing = client;
    if (existing == null) {
      synchronized (this) {
        existing = client;
        if (existing == null) {
          existing = HttpClient.newBuilder().connectTimeout(callTimeout).build();
          client = existing;
        }
      }
    }
    return existing;
  }

  /**
   * The bound, applied in BYTES rather than characters because the column and the reason for the
   * bound are both about size. Cut on a character boundary, then say so — a silently cut JSON
   * document reads as a malformed one, which is a bug report about the wrong service.
   */
  private static String bound(String body) {
    if (body == null) {
      return null;
    }
    if (body.getBytes(StandardCharsets.UTF_8).length <= RESPONSE_LIMIT_BYTES) {
      return body;
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    int end = RESPONSE_LIMIT_BYTES;
    // Do not split a UTF-8 sequence: walk back off any continuation byte.
    while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
      end--;
    }
    return new String(bytes, 0, end, StandardCharsets.UTF_8) + TRUNCATION_MARKER;
  }

  /** The body as a tree, or null. A truncated body does not parse, and that is not an error. */
  private static JsonNode parse(String body) {
    if (body == null || body.isBlank()) {
      return null;
    }
    try {
      return JSON.readTree(body);
    } catch (Exception e) {
      return null;
    }
  }

  private static String trimTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /** The configured base urls, for the {@code GET /processes} listing and for tests. */
  public Optional<String> configured(String target) {
    try {
      return Optional.ofNullable(base(target));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
