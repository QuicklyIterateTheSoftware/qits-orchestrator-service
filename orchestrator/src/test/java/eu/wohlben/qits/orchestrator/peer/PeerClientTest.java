package eu.wohlben.qits.orchestrator.peer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The real client against a loopback server — the half {@link FakePeers} cannot prove: the headers
 * that go out, the bound on a body, and what an unreachable peer comes back as.
 *
 * <p>A plain JDK {@code HttpServer} rather than WireMock: it is three lines, it needs no dependency,
 * and what is under test is one class making one request.
 *
 * <p>Not a {@code @QuarkusTest}: the client is constructed here with its four url fields and its
 * timeout set directly, so nothing about this depends on an application booting.
 */
class PeerClientTest {

  private HttpServer server;

  private final List<String[]> seenHeaders = new ArrayList<>();

  private volatile String responseBody = "{\"ok\":true}";

  private volatile int responseStatus = 200;

  private PeerClient client;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::answer);
    server.start();

    client = new PeerClient();
    String base = "http://127.0.0.1:" + server.getAddress().getPort();
    client.artifactsUrl = base;
    client.containersUrl = base;
    client.ciUrl = base;
    client.deploymentsUrl = base;
    client.callTimeout = Duration.ofSeconds(10);
    client.tokens =
        new PeerTokens() {
          @Override
          public Optional<String> token(String target) {
            return Optional.of("a-minted-token");
          }
        };
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private void answer(HttpExchange exchange) throws java.io.IOException {
    seenHeaders.add(
        new String[] {
          exchange.getRequestHeaders().getFirst("X-Qits-User"),
          exchange.getRequestHeaders().getFirst("X-Qits-Roles"),
          exchange.getRequestHeaders().getFirst("Authorization")
        });
    byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(responseStatus, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }

  @Test
  void everyCallCarriesTheForwardAuthPairAndTheBearerWhenOneCanBeMinted() {
    PeerExchange exchange = client.get(PeerTarget.CONTAINERS, "/containers/api/gc/usage");

    assertTrue(exchange.answer().ok());
    assertEquals(200, exchange.answer().httpStatus());
    assertNotNull(exchange.answer().json());
    assertEquals(1, seenHeaders.size());
    assertEquals("qits-platform-orchestrator", seenHeaders.getFirst()[0]);
    assertEquals("qits:system", seenHeaders.getFirst()[1]);
    assertEquals("Bearer a-minted-token", seenHeaders.getFirst()[2]);
  }

  @Test
  void aPostSendsTheBodyItRecordsAndTheUrlIsTheTargetPlusThePath() {
    PeerExchange exchange =
        client.post(PeerTarget.ARTIFACTS, "/artifacts/api/gc/plan", "{\"pins\":{}}");

    assertEquals("POST", exchange.call().method());
    assertEquals("{\"pins\":{}}", exchange.call().body());
    assertTrue(exchange.call().url().endsWith("/artifacts/api/gc/plan"), exchange.call().url());
  }

  @Test
  void aBodyOverTheBoundIsCutOnACharacterBoundaryAndSaysSo() {
    // One character over, with a multi-byte character straddling the cut so a naive substring would
    // produce a replacement character rather than a clean end.
    responseBody = "\"" + "é".repeat(PeerClient.RESPONSE_LIMIT_BYTES) + "\"";

    PeerExchange exchange = client.get(PeerTarget.CI, "/ci/api/daemon");

    String stored = exchange.answer().body();
    assertTrue(stored.contains("truncated by qits-platform-orchestrator"), "no marker in the body");
    assertTrue(
        stored.length() < responseBody.length(), "a body over the bound must be shorter than it was");
    // Only the STORED copy is cut. The tree is parsed from the whole answer (qits-1175).
    assertNotNull(exchange.answer().json());
    assertEquals(PeerClient.RESPONSE_LIMIT_BYTES, exchange.answer().json().asText().length());
    assertFalse(exchange.answer().truncated());
    assertNull(exchange.answer().error());
  }

  @Test
  void aPinAnswerOverOneMebibyteParsesWhole() {
    // The shape that broke run cd1c3349: maintenance's pin answer past 1 MiB. Every pin must be in
    // the tree, the last one included — the stored copy is cut, the parsed answer is not.
    StringBuilder body = new StringBuilder("{\"pins\":[");
    int count = 20_000;
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        body.append(',');
      }
      body.append("{\"name\":\"eu.wohlben.qits:lib-")
          .append(i)
          .append("\",\"version\":\"2026.1010.")
          .append(i)
          .append("\",\"repository\":\"qits-some-repository-service\"}");
    }
    body.append("]}");
    responseBody = body.toString();
    assertTrue(responseBody.length() > PeerClient.RESPONSE_LIMIT_BYTES, "the fixture must pass 1 MiB");

    PeerAnswer answer = client.get(PeerTarget.CI, "/ci/api/daemon").answer();

    assertFalse(answer.truncated());
    assertNotNull(answer.json());
    assertEquals(count, answer.json().get("pins").size());
    assertEquals(
        "eu.wohlben.qits:lib-" + (count - 1),
        answer.json().get("pins").get(count - 1).get("name").asText());
    assertTrue(answer.body().contains("truncated by qits-platform-orchestrator"));
  }

  @Test
  void anAnswerOverTheParseLimitIsMarkedTruncatedWithNoTree() {
    client.parseLimitBytes = 1024;
    responseBody = "{\"pins\":[\"" + "x".repeat(4096) + "\"]}";

    PeerAnswer answer = client.get(PeerTarget.CI, "/ci/api/daemon").answer();

    assertEquals(200, answer.httpStatus());
    assertTrue(answer.truncated(), "an answer past the parse limit must say so");
    assertNull(answer.json());
    assertNull(answer.error());
  }

  @Test
  void anUnreachablePeerIsAnAnswerWithASentenceRatherThanAnException() {
    // A port nothing listens on: the server above is still up, so this is a refused connection
    // rather than a hang.
    client.ciUrl = "http://127.0.0.1:" + freePort();

    PeerAnswer answer = client.get(PeerTarget.CI, "/ci/api/daemon").answer();

    assertNull(answer.httpStatus());
    assertNotNull(answer.error());
    assertTrue(answer.error().contains("could not be called"), answer.error());
  }

  @Test
  void aNonJsonBodyIsKeptAsTextAndTheTreeIsSimplyAbsent() {
    responseBody = "<html>a gateway said no</html>";
    responseStatus = 502;

    PeerAnswer answer = client.get(PeerTarget.CONTAINERS, "/containers/api/gc/usage").answer();

    assertEquals(502, answer.httpStatus());
    assertEquals("<html>a gateway said no</html>", answer.body());
    assertNull(answer.json());
    assertNull(answer.error());
  }

  private static int freePort() {
    try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
