package eu.wohlben.qits.orchestrator.peer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

/**
 * Every peer and the idp as the DEPLOYED process resolves them (qits-1423, feature qits-1406): the
 * prod profile of the two shipped files, read at their real ordinals with no {@code %test} line in
 * play. The suites run on {@code %test}'s local values, so this is the only place the edge hosts
 * themselves are under test.
 *
 * <p>Three claims: each address is {@code https://<label>.qits.<domain>} with the peer's {@code
 * host:} as the label; no shipped line names {@code QITS_ENVIRONMENT} or a {@code
 * <env>-qits-<app>:8080} wire alias; and without {@code QITS_DOMAIN} the address does not resolve
 * at all — there is no fallback domain to send a deletion to.
 */
class EdgeAddressShippedConfigTest {

  private static final String LIBRARY = "META-INF/microprofile-config.properties";
  private static final String APPLICATION = "application.properties";

  /** Each target key and the edge host it must ship, for {@code QITS_DOMAIN=example.test}. */
  private static final Map<String, String> TARGETS = new LinkedHashMap<>();

  static {
    TARGETS.put("qits.orchestrator.targets.artifacts-url", "https://registry.qits.example.test");
    TARGETS.put("qits.orchestrator.targets.containers-url", "https://containers.qits.example.test");
    TARGETS.put("qits.orchestrator.targets.ci-url", "https://ci.qits.example.test");
    TARGETS.put(
        "qits.orchestrator.targets.deployments-url", "https://deployments.qits.example.test");
    TARGETS.put("qits.orchestrator.targets.projects-url", "https://projects.qits.example.test");
    TARGETS.put("qits.orchestrator.targets.workspaces-url", "https://workspaces.qits.example.test");
    TARGETS.put(
        "qits.orchestrator.targets.maintenance-url", "https://maintenance.qits.example.test");
    TARGETS.put("qits.orchestrator.targets.idp-url", "https://idp.qits.example.test");
  }

  @Test
  void everyPeerIsItsEdgeHost() {
    SmallRyeConfig config = prod(Map.of("QITS_DOMAIN", "example.test"));
    TARGETS.forEach((key, expected) -> assertEquals(expected, config.getRawValue(key), key));
  }

  @Test
  void theIdpIsItsEdgeHostForBothTheTenantAndTheClient() {
    SmallRyeConfig config = prod(Map.of("QITS_DOMAIN", "example.test"));
    assertEquals(
        "https://idp.qits.example.test/idp", config.getRawValue("quarkus.oidc.auth-server-url"));
    assertEquals(
        "https://idp.qits.example.test/idp",
        config.getRawValue("quarkus.oidc-client.qits.auth-server-url"));
  }

  @Test
  void theDeployersIdpResourceStillWinsForTheClient() {
    SmallRyeConfig config =
        prod(
            Map.of(
                "QITS_DOMAIN", "example.test", "QITS_RESOURCE_IDP_URL", "http://resource-idp/idp"));
    assertEquals(
        "http://resource-idp/idp", config.getRawValue("quarkus.oidc-client.qits.auth-server-url"));
  }

  /** {@code getValue}, not {@code getRawValue}: a raw read answers null when it cannot expand. */
  @Test
  void withoutTheDomainNothingResolves() {
    SmallRyeConfig config = prod(Map.of());
    for (String key : TARGETS.keySet()) {
      assertThrows(NoSuchElementException.class, () -> config.getValue(key, String.class), key);
    }
    assertThrows(
        NoSuchElementException.class,
        () -> config.getValue("quarkus.oidc.auth-server-url", String.class));
    assertThrows(
        NoSuchElementException.class,
        () -> config.getValue("quarkus.oidc-client.qits.auth-server-url", String.class));
  }

  @Test
  void noShippedPeerAddressNamesAnEnvironment() {
    for (String file : new String[] {LIBRARY, APPLICATION}) {
      Properties shipped = load(file);
      for (String key : shipped.stringPropertyNames()) {
        boolean peerAddress =
            key.contains("qits.orchestrator.targets.") || key.endsWith(".auth-server-url");
        if (!peerAddress) {
          continue;
        }
        String value = shipped.getProperty(key);
        assertFalse(value.contains("QITS_ENVIRONMENT"), file + " " + key + "=" + value);
        assertFalse(value.contains("${QITS_DOMAIN:"), file + " " + key + " defaults the domain");
      }
    }
  }

  /** The two shipped files at the ordinals Quarkus gives them, under the prod profile. */
  private static SmallRyeConfig prod(Map<String, String> environment) {
    return new SmallRyeConfigBuilder()
        .withProfile("prod")
        .withSources(source(LIBRARY, 100), source(APPLICATION, 250))
        .withSources(new PropertiesConfigSource(environment, "environment", 300))
        .build();
  }

  private static ConfigSource source(String resource, int ordinal) {
    URL url = main(resource);
    try {
      return new PropertiesConfigSource(url, ordinal);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Properties load(String resource) {
    Properties properties = new Properties();
    try (InputStream in = main(resource).openStream()) {
      properties.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return properties;
  }

  /**
   * The SHIPPED copy of a resource: several jars on this classpath carry a {@code
   * META-INF/microprofile-config.properties}, and the test classpath has an application.properties
   * of its own, so the one meant is the one that declares the key it owns.
   */
  private static URL main(String resource) {
    String owned =
        LIBRARY.equals(resource) ? "qits.orchestrator.targets." : "quarkus.oidc.auth-server-url";
    try {
      var urls = EdgeAddressShippedConfigTest.class.getClassLoader().getResources(resource);
      while (urls.hasMoreElements()) {
        URL url = urls.nextElement();
        Properties candidate = new Properties();
        try (InputStream in = url.openStream()) {
          candidate.load(in);
        }
        if (candidate.stringPropertyNames().stream().anyMatch(k -> k.startsWith(owned))) {
          return url;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalStateException("no shipped " + resource + " on the classpath");
  }
}
