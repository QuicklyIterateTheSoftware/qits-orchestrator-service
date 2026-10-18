package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import au.com.dius.pact.consumer.ConsumerPactRunnerKt;
import au.com.dius.pact.consumer.PactVerificationResult;
import au.com.dius.pact.consumer.model.MockProviderConfig;
import au.com.dius.pact.core.model.PactSpecVersion;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.peer.PeerTokens;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * <b>The client-credentials token mint, as quarkus-oidc-client makes it</b> (ticket qits-1149):
 * the shipped {@code qits} named client, switched on and pointed at a pact mock server, asked for a
 * token through {@link PeerTokens}, the one bean that spends it on every peer call. The row is
 * {@link GcContract#TOKEN_MINT}; it runs here because the oidc client only exists inside the
 * application, on a port chosen before the application starts, because the client reads its idp
 * from config.
 *
 * <p>The client authenticates with Basic as the state's client and sends {@code
 * grant_type=client_credentials&audience=qits-platform} — the shipped grant options — which is what
 * qits-idp's {@code a service client calling its peers} records.
 */
@QuarkusTest
@TestProfile(IdpTokenMintPactTest.MockIdp.class)
class IdpTokenMintPactTest {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  private static final String PORT_PROPERTY = "qits.test.idp-token-mint-port";

  /**
   * The port the pact mock server listens on, chosen free once per JVM and kept in a system
   * property: a {@code QuarkusTestProfile} is instantiated in two classloaders, and a static field
   * is not shared between them.
   */
  static final int PORT = port();

  private static int port() {
    synchronized (System.class) {
      String chosen = System.getProperty(PORT_PROPERTY);
      if (chosen == null) {
        chosen = Integer.toString(freePort());
        System.setProperty(PORT_PROPERTY, chosen);
      }
      return Integer.parseInt(chosen);
    }
  }

  /** The {@code qits} client on, as the state's caller, against the mock server. */
  public static class MockIdp implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      String[] caller = basicPair();
      return Map.of(
          "quarkus.oidc-client.qits.client-enabled", "true",
          "quarkus.oidc-client.qits.auth-server-url", "http://127.0.0.1:" + PORT + "/idp",
          "quarkus.oidc-client.qits.client-id", caller[0],
          "quarkus.oidc-client.qits.credentials.secret", caller[1]);
    }
  }

  @Inject PeerTokens tokens;

  @Test
  void thePeerBearerIsMintedFromTheIdp() {
    var pact = GcContract.pact(PeerTarget.IDP);
    String expected =
        GcContract.IDP.json(GcContract.A_PEER_CALLER, "issueToken").path("access_token").asText();
    PactVerificationResult result =
        ConsumerPactRunnerKt.runConsumerTest(
            pact.pact(List.of(GcContract.TOKEN_MINT)),
            MockProviderConfig.httpConfig("127.0.0.1", PORT, PactSpecVersion.V4),
            (mockServer, context) -> {
              assertEquals(
                  Optional.of(expected),
                  tokens.token(PeerTarget.IDP),
                  "the idp's access_token is the bearer");
              return null;
            });
    if (result instanceof PactVerificationResult.Error error
        && error.getError() instanceof AssertionError assertion) {
      throw assertion;
    }
    if (!(result instanceof PactVerificationResult.Ok)) {
      fail(GcContract.TOKEN_MINT.description() + ": " + result);
    }
  }

  /** The client id and secret the state's {@code authorization} param is the Basic header of. */
  private static String[] basicPair() {
    String basic =
        GcContract.IDP
            .params(GcContract.A_PEER_CALLER)
            .get("authorization")
            .substring("Basic ".length());
    return new String(Base64.getDecoder().decode(basic), StandardCharsets.UTF_8).split(":", 2);
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
