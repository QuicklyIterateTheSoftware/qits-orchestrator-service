package eu.wohlben.qits.orchestrator.peer;

import eu.wohlben.qits.http.QitsHttpClients;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.Optional;

/**
 * Peer clients outside CDI for the gc process's consumer pacts (ticket qits-1149): the real {@link
 * PeerClient} against a pact mock server, and a {@link FakePeers} whose every peer has a url of its
 * own, so a recorded call says which peer it went to.
 */
public final class ContractPeers {

  private ContractPeers() {}

  /** The real client with every peer at {@code baseUrl}. No token: the mock checks the request. */
  public static PeerClient against(String baseUrl) {
    PeerClient client = wire(new PeerClient(), target -> baseUrl);
    client.clients = QitsHttpClients.of(OpenTelemetry.noop(), null);
    return client;
  }

  /**
   * A fake whose peer {@code <target>} sits at {@code http://<target>}, so {@link #target} reads
   * the peer back off a recorded call's url.
   */
  public static FakePeers fake() {
    return wire(new FakePeers(), target -> "http://" + target);
  }

  /** The peer a {@link #fake()} call went to: the host of its url. */
  public static String target(PeerCall call) {
    String rest = call.url().substring("http://".length());
    return rest.substring(0, rest.indexOf('/'));
  }

  /** The path of a {@link #fake()} call, without its peer. */
  public static String path(PeerCall call) {
    String rest = call.url().substring("http://".length());
    return rest.substring(rest.indexOf('/'));
  }

  private static <T extends PeerClient> T wire(
      T client, java.util.function.Function<String, String> url) {
    client.artifactsUrl = url.apply(PeerTarget.ARTIFACTS);
    client.containersUrl = url.apply(PeerTarget.CONTAINERS);
    client.ciUrl = url.apply(PeerTarget.CI);
    client.deploymentsUrl = url.apply(PeerTarget.DEPLOYMENTS);
    client.projectsUrl = url.apply(PeerTarget.PROJECTS);
    client.workspacesUrl = url.apply(PeerTarget.WORKSPACES);
    client.maintenanceUrl = url.apply(PeerTarget.MAINTENANCE);
    client.configurationUrl = url.apply(PeerTarget.CONFIGURATION);
    client.idpUrl = url.apply(PeerTarget.IDP);
    client.callTimeout = Duration.ofSeconds(10);
    client.tokens =
        new PeerTokens() {
          @Override
          public Optional<String> token(String target) {
            return Optional.empty();
          }
        };
    return client;
  }
}
