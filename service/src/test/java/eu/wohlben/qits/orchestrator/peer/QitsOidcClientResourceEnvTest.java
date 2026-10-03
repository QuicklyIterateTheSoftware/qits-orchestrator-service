package eu.wohlben.qits.orchestrator.peer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

/**
 * Once a deployment declares {@code idp:client} (epic qits-540 dossier, 'Plan (as of 2026-09-13)',
 * C5) qits-deployments injects {@code QITS_RESOURCE_IDP_URL}, {@code QITS_RESOURCE_IDP_CLIENT_ID}
 * and {@code QITS_RESOURCE_IDP_CLIENT_SECRET}. The {@code qits} client reads those three and
 * nothing else — there is no old extras fallback left to win against.
 */
@QuarkusTest
@TestProfile(QitsOidcClientResourceEnvTest.ResourceTripleSet.class)
class QitsOidcClientResourceEnvTest {

  public static class ResourceTripleSet implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "QITS_RESOURCE_IDP_CLIENT_ID", "resource-dev-qits-orchestrator",
          "QITS_RESOURCE_IDP_CLIENT_SECRET", "resource-secret",
          "QITS_RESOURCE_IDP_URL", "http://resource-idp:8080/idp");
    }
  }

  private static String value(String key) {
    Config config = ConfigProvider.getConfig();
    return config.getValue(key, String.class);
  }

  @Test
  void theResourceEnvResolvesTheQitsClient() {
    assertEquals("resource-dev-qits-orchestrator", value("quarkus.oidc-client.qits.client-id"));
    assertEquals("resource-secret", value("quarkus.oidc-client.qits.credentials.secret"));
    assertEquals("http://resource-idp:8080/idp", value("quarkus.oidc-client.qits.auth-server-url"));
  }
}
