package eu.wohlben.qits.orchestrator.peer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

/**
 * The one named oidc client, {@code qits}, as the shipped {@code microprofile-config.properties}
 * resolves it with no {@code QITS_RESOURCE_IDP_*} set — the "nothing configured" arm every
 * clone-alone build and every other test in this repository runs on (epic qits-540 dossier, 'Plan
 * (as of 2026-09-13)', C4).
 *
 * <p>{@link QitsOidcClientResourceEnvTest} holds the other arm, the deployer's resource triple
 * winning over the shipped defaults, in its own {@code @QuarkusTest} because a {@code
 * @TestProfile}'s config overrides are fixed for the life of one boot.
 */
@QuarkusTest
class QitsOidcClientShippedConfigTest {

  private static String value(String key) {
    Config config = ConfigProvider.getConfig();
    return config.getValue(key, String.class);
  }

  @Test
  void theQitsClientResolvesItsOwnDefaults() {
    // The host is derived off QITS_ENVIRONMENT, which no test sets, so the `dev` fallback applies —
    // the application is qits-idp (it was qits-platform-idp until the platform tier went), so its
    // alias is dev-qits-idp.
    assertEquals(
        "http://dev-qits-idp:8080/idp", value("quarkus.oidc-client.qits.auth-server-url"));
    assertEquals("dev-qits-orchestrator", value("quarkus.oidc-client.qits.client-id"));
    // Empty, not absent — SmallRye reads a configured-empty String as null, so an empty secret reads
    // as an empty Optional rather than as "" itself.
    Optional<String> secret =
        ConfigProvider.getConfig()
            .getOptionalValue("quarkus.oidc-client.qits.credentials.secret", String.class);
    assertTrue(secret.isEmpty());
    // One audience for every peer: the platform's, which is the only one the idp mints.
    assertEquals("qits-platform", value("quarkus.oidc-client.qits.grant-options.client.audience"));
  }

  @Test
  void theClientStaysDisabledUnderTest() {
    // %test.quarkus.oidc-client.qits.client-enabled=false wins over the shipped `true` — the arm
    // every test in this repository is on, so a suite never dials a real idp.
    assertEquals("false", value("quarkus.oidc-client.qits.client-enabled"));
  }

  @Test
  void theSixNamesTheDeploymentStillSetsAreNeutralised() {
    // No code mints through these — `qits` mints for every peer — but the deployed configuration
    // still carries a QUARKUS_OIDC_CLIENT_<NAME>_* family for each (dev-qits-orchestrator's envKeys,
    // read 2026-10-02; `artifacts` included now that it is no longer the `qits` client's fallback),
    // _CLIENT_ENABLED=true among them, and ONE variable of a family is enough to mint
    // `quarkus.oidc-client.<name>` as a map key in the environment source. With no block behind it
    // the name answers the extension's defaults, and both are ON: an enabled, discovering client is
    // built during runtime init and blocks on metadata discovery for `connection-timeout` per
    // client, before the listener accepts, so an issuer that accepts and does not answer fails this
    // service's boot.
    //
    // All three keys are load-bearing, and that is what this pins. `client-enabled=false` is
    // overridden by the deployment's own _CLIENT_ENABLED=true (the environment outranks this file),
    // so `discovery-enabled=false` — the key no deployment sets — is what actually keeps an enabled
    // client off the network, and `token-path` is what stops discovery-off from throwing a
    // ConfigurationException for want of a token endpoint. Drop any one of the three and the boot
    // hazard is back.
    String[] neutralised = {
      PeerTarget.ARTIFACTS,
      PeerTarget.CI,
      PeerTarget.CONTAINERS,
      PeerTarget.DEPLOYMENTS,
      PeerTarget.PROJECTS,
      PeerTarget.WORKSPACES
    };
    for (String peer : neutralised) {
      assertEquals(
          "false",
          value("quarkus.oidc-client." + peer + ".client-enabled"),
          peer + " must ship client-enabled=false");
      assertEquals(
          "false",
          value("quarkus.oidc-client." + peer + ".discovery-enabled"),
          peer + " must ship discovery-enabled=false — the key the deployment cannot override");
      assertEquals(
          "token",
          value("quarkus.oidc-client." + peer + ".token-path"),
          peer + " must ship a token-path, or discovery-enabled=false fails the boot");
    }
  }

  @Test
  void thereIsNoBlockForTheTwoNamesNoDeploymentSets() {
    // qits-platform-maintenance, qits-configuration and qits-idp have no QUARKUS_OIDC_CLIENT_
    // MAINTENANCE_*, _CONFIGURATION_* or _IDP_* entry anywhere, so no map key is minted for any of
    // those names and there is nothing
    // to neutralise. What says a block is absent is that the keys answer the extension's own
    // defaults — `client-enabled` is `true` and `discovery-enabled` has no value at all for ANY
    // name, invented ones included, because these are a config MAPPING's defaults rather than a
    // client somebody declared. A shipped `false` here would mean a block came back.
    for (String peer : new String[] {PeerTarget.MAINTENANCE, PeerTarget.CONFIGURATION, PeerTarget.IDP}) {
      assertEquals(
          "true",
          value("quarkus.oidc-client." + peer + ".client-enabled"),
          "there must be no shipped " + peer + " oidc client block");
      assertTrue(
          ConfigProvider.getConfig()
              .getOptionalValue("quarkus.oidc-client." + peer + ".discovery-enabled", String.class)
              .isEmpty(),
          "there must be no shipped " + peer + " oidc client block");
    }
  }
}
