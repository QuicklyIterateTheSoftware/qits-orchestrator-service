package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.pact.consumer.ConsumerPact;
import eu.wohlben.qits.pact.consumer.GoldenInteraction;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * <b>The consumer half of the gc process's contracts</b> (ticket qits-1149): for each row of
 * {@link GcContract}, the real step, with a real {@link eu.wohlben.qits.orchestrator.peer.PeerClient}
 * making a real HTTP call, against a pact-jvm mock server answering the row's interaction — only
 * the fields the row {@code consumes}. The mock server fails the row unless the step sends the
 * provider's recorded request.
 *
 * <p>The step must SUCCEED against the mock. Separately, the line it makes from the provider's
 * recorded answer cut to {@code consumes} must be the line it makes from the whole answer: a field
 * the summary reads but {@code consumes} leaves out shows up as a different line. (The mock's own
 * answer cannot be compared that way: an array matched by type answers its first element's values
 * for every element, so a sum over it differs from the recording's.)
 *
 * <p>The qits-idp pact's two tenant rows ({@link GcContract#TENANT_READS}, qits-708) are no gc
 * step: the service's quarkus-oidc tenant makes them at startup. They are read here as quarkus-oidc
 * reads them — the discovery {@code issuer} compared by value, the JWKS key named by the recorded
 * {@code kid} built into an RSA public key.
 *
 * <p>Then, per peer, the committed {@code pacts/qits-orchestrator-service_<provider>.json} is what
 * the rows write ({@code -Dgolden.update=true} rewrites it), and every interaction carries both
 * references.
 */
class GcConsumerPactTest {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  /** Every {@link PeerTarget}, in the order the pacts are reported. */
  static final List<String> TARGETS =
      List.of(
          PeerTarget.CONTAINERS,
          PeerTarget.ARTIFACTS,
          PeerTarget.DEPLOYMENTS,
          PeerTarget.CI,
          PeerTarget.MAINTENANCE,
          PeerTarget.WORKSPACES,
          PeerTarget.PROJECTS,
          PeerTarget.IDP);

  @TestFactory
  Stream<DynamicTest> everyRowIsWhatTheStepAsksAndReads() {
    return GcContract.ROWS.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.stepId() + " -> " + GcContract.masters(row).repository() + " "
                        + row.interaction().operationId(),
                    () -> verify(row)));
  }

  private static void verify(GcContract.Row row) {
    StepResult whole = GcContract.againstRecording(row);
    assertEquals(RunStatus.SUCCEEDED, whole.status(), String.valueOf(whole.error()));
    assertEquals(
        whole.summary(),
        GcContract.againstConsumed(row).summary(),
        "consumes misses a field the step reads");
    GcContract.pact(row.target())
        .run(
            row.interaction(),
            (url, recorded) -> {
              StepResult actual =
                  GcContract.run(row, ContractPeers.against(url, GcContract.bearer(row)));
              assertEquals(RunStatus.SUCCEEDED, actual.status(), String.valueOf(actual.error()));
              assertNotNull(actual.summary(), "the step reads the pact's answer");
            });
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  @TestFactory
  Stream<DynamicTest> theOidcTenantsStartupReadsAreWhatQuarkusOidcReads() {
    ConsumerPact pact = GcContract.pact(PeerTarget.IDP);
    return Stream.of(
        DynamicTest.dynamicTest(
            "quarkus-oidc tenant -> qits-idp getOpenIdConfiguration",
            () ->
                pact.run(
                    GcContract.IDP_DISCOVERY,
                    (url, recorded) -> {
                      JsonNode document = JSON.readTree(get(url + recorded.examplePath()));
                      assertEquals(
                          recordedJson(GcContract.IDP_DISCOVERY).path("issuer").asText(),
                          document.path("issuer").asText(),
                          "the issuer every bearer's iss is checked against");
                      assertTrue(
                          URI.create(document.path("jwks_uri").asText()).isAbsolute(),
                          "jwks_uri is where the tenant fetches the keys");
                      URI.create(document.path("token_endpoint").asText());
                    })),
        DynamicTest.dynamicTest(
            "quarkus-oidc tenant -> qits-idp getJwks",
            () ->
                pact.run(
                    GcContract.IDP_JWKS,
                    (url, recorded) -> {
                      String kid =
                          recordedJson(GcContract.IDP_JWKS).path("keys").path(0).path("kid").asText();
                      JsonNode key = null;
                      for (JsonNode candidate :
                          JSON.readTree(get(url + recorded.examplePath())).path("keys")) {
                        if (kid.equals(candidate.path("kid").asText())) {
                          key = candidate;
                        }
                      }
                      assertNotNull(key, "the JWKS names the recorded kid " + kid);
                      assertEquals("RSA", key.path("kty").asText());
                      assertEquals("RS256", key.path("alg").asText());
                      assertEquals("sig", key.path("use").asText());
                      Base64.Decoder b64 = Base64.getUrlDecoder();
                      RSAPublicKey rsa =
                          (RSAPublicKey)
                              KeyFactory.getInstance("RSA")
                                  .generatePublic(
                                      new RSAPublicKeySpec(
                                          new BigInteger(1, b64.decode(key.path("n").asText())),
                                          new BigInteger(1, b64.decode(key.path("e").asText()))));
                      assertTrue(rsa.getModulus().bitLength() >= 2048, "a usable RS256 key");
                    })));
  }

  private static JsonNode recordedJson(GoldenInteraction row) {
    return GcContract.IDP.json(row.state(), row.operationId());
  }

  /** A plain GET of the mock server, as quarkus-oidc's own reads are: 200 and the body. */
  private static String get(String url) throws Exception {
    HttpResponse<String> answer =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(url)).header("Accept", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, answer.statusCode(), answer.body());
    return answer.body();
  }

  /**
   * Each row of {@link GcContract#PENDING}, reported skipped with the state it is waiting for — the
   * same shape {@code ProjectsConsumerPactTest} (qits-maintenance-service) reports its own pending
   * rows with. Not a failure: the state is not on the classpath yet because qits-artifacts has not
   * published it, and a row here is a promise to switch it on once it does.
   */
  @TestFactory
  Stream<DynamicTest> everyPendingRowWaitsForItsProviderState() {
    return GcContract.PENDING.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.stepId() + " -> " + row.operationId() + " [" + row.state() + "]",
                    () -> Assumptions.abort(row.reason())));
  }

  /**
   * The artifacts cleanup doors' calls (ticket qits-2138, {@link GcContract#CLEANUP}): each door,
   * with the five pins answering the recorded request's members, must send qits-artifacts exactly
   * the recorded request and get the recorded status back. A row whose operation the pinned golden
   * masters do not record yet is reported skipped with what it needs, the same shape as {@link
   * #everyPendingRowWaitsForItsProviderState}, and switches itself on with the pin bump.
   */
  @TestFactory
  Stream<DynamicTest> everyCleanupDoorAsksWhatQitsArtifactsRecorded() {
    return GcContract.CLEANUP.stream()
        .map(
            row ->
                DynamicTest.dynamicTest(
                    row.door() + " -> " + GcContract.ARTIFACTS.repository() + " "
                        + row.interaction().operationId(),
                    () -> {
                      if (!GcContract.recorded(row)) {
                        Assumptions.abort(
                            GcContract.pact(PeerTarget.ARTIFACTS).needs(row.interaction()));
                      }
                      GcContract.pact(PeerTarget.ARTIFACTS)
                          .run(
                              row.interaction(),
                              (url, recorded) -> {
                                var exchange =
                                    GcContract.runCleanup(row, ContractPeers.against(url));
                                assertEquals(
                                    recorded.status(),
                                    exchange.answer().httpStatus(),
                                    String.valueOf(exchange.answer().error()));
                              });
                    }));
  }

  @TestFactory
  Stream<DynamicTest> theCommittedPactsAreWhatTheRowsWrite() {
    return TARGETS.stream()
        .map(
            target -> {
              ConsumerPact pact = GcContract.pact(target);
              return DynamicTest.dynamicTest(
                  pact.file(),
                  () -> {
                    pact.compareOrWritePactFile();
                    pact.assertEveryInteractionCarriesBothReferences();
                  });
            });
  }
}
