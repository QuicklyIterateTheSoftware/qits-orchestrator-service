package eu.wohlben.qits.orchestrator.peer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.Tokens;
import io.smallrye.mutiny.Uni;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code PeerTokens} with the one named client, {@code qits}, faked directly — no CDI, the same
 * plain-construction shape {@code PeerClientTest} uses for {@code PeerClient}. What this proves is
 * the behaviour change the epic qits-540 dossier's 'Plan (as of 2026-09-13)', C4, makes: ONE client
 * now answers {@link PeerTokens#token(String)} for every one of the eight peers, {@code maintenance}
 * and {@code configuration} included — the two that {@code ComposeTemplate.java} never turned a
 * bearer on for before this commit, because a live deployment only ever enabled six of the eight
 * old clients.
 *
 * <p>The config keys themselves — the shipped defaults, and the deployer's resource triple — are
 * {@link QitsOidcClientShippedConfigTest} and {@link QitsOidcClientResourceEnvTest}; this class is
 * the runtime behaviour once the client is enabled.
 */
class PeerTokensTest {

  private static final String ENABLED_KEY = "quarkus.oidc-client.qits.client-enabled";

  @AfterEach
  void clearTheSwitch() {
    System.clearProperty(ENABLED_KEY);
  }

  @Test
  void everyPeerReadsTheSameBearerFromTheOneClientIncludingMaintenanceAndConfiguration() {
    System.setProperty(ENABLED_KEY, "true");
    PeerTokens tokens = new PeerTokens();
    tokens.qits = fakeClient("a-minted-token");

    String[] everyPeer = {
      PeerTarget.ARTIFACTS,
      PeerTarget.CONTAINERS,
      PeerTarget.CI,
      PeerTarget.DEPLOYMENTS,
      PeerTarget.PROJECTS,
      PeerTarget.WORKSPACES,
      // The two that a live deployment never turned a bearer on for before this commit
      // (ComposeTemplate.java only ever enabled six of the eight old clients).
      PeerTarget.MAINTENANCE,
      PeerTarget.CONFIGURATION
    };
    for (String target : everyPeer) {
      assertEquals(
          Optional.of("a-minted-token"), tokens.token(target), "no bearer for target " + target);
    }
  }

  @Test
  void aDisabledClientAnswersEmptyForEveryPeer() {
    System.setProperty(ENABLED_KEY, "false");
    PeerTokens tokens = new PeerTokens();
    tokens.qits = fakeClient("never asked for");

    assertTrue(tokens.token(PeerTarget.MAINTENANCE).isEmpty());
    assertTrue(tokens.token(PeerTarget.CONFIGURATION).isEmpty());
  }

  private static OidcClient fakeClient(String accessToken) {
    long farFuture = (System.currentTimeMillis() / 1000) + 3600;
    Tokens minted = new Tokens(accessToken, farFuture, null, null, null, null, "qits");
    return new OidcClient() {
      @Override
      public Uni<Tokens> getTokens(Map<String, String> additionalGrantParameters) {
        return Uni.createFrom().item(minted);
      }

      @Override
      public Uni<Tokens> refreshTokens(
          String refreshToken, Map<String, String> additionalGrantParameters) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public Uni<Boolean> revokeAccessToken(
          String token, Map<String, String> additionalParameters) {
        throw new UnsupportedOperationException("not exercised by this test");
      }

      @Override
      public void close() {}
    };
  }
}
