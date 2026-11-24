package eu.wohlben.qits.orchestrator.contracts;

import static org.junit.jupiter.api.Assertions.fail;

import au.com.dius.pact.core.model.Interaction;
import au.com.dius.pact.core.model.Pact;
import au.com.dius.pact.core.model.ProviderState;
import au.com.dius.pact.provider.junit5.HttpTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactSource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import jakarta.inject.Inject;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * <b>Verifies every consumer's pact against the running qits-orchestrator</b>, the way
 * qits-stt-service, qits-edge-service and qits-observability-service do.
 *
 * <p>The pacts come off the test classpath: each consumer publishes its pact as a jar holding
 * {@code pacts/<consumer>_qits-orchestrator-service.json} (repository names on both sides), this
 * repo pins that jar as a test dependency, and qits-maintenance bumps the pin when the consumer
 * releases a changed pact. {@link ClasspathPactLoader} finds them all.
 *
 * <p><b>qits-landing-app is the pinned consumer</b> (ticket qits-2329): its pact, {@code
 * pacts/qits-landing-app_qits-orchestrator-service.json}, pins the process page's states (its
 * cleanup page and the two cleanup states went in qits-2477). {@link ClasspathPactLoader#REQUIRED} is true,
 * so this class no longer carries {@code @IgnoreNoPactsToVerify}: a classpath that lost the pin now
 * fails rather than silently verifying nothing.
 *
 * <p>Each interaction runs against this {@code @QuarkusTest} application over real HTTP, as an
 * operator holding {@code qits:admin} ({@code @TestSecurity}), with the peers faked at {@code
 * FakePeers} and the run log fresh ({@link ContractProfile}, shared with {@link
 * GoldenMasterRecordingTest}). Every {@code @State} method delegates to {@link ProviderStates};
 * {@link #target} fails an unknown state, and an interaction without {@code
 * comments.references.qits-call} or {@code qits-trigger}. {@link #drain} waits out the run a
 * verified {@code startProcessRun} leaves on the executor, as the recording does, and closes
 * the run {@code a running gc run} seeds.
 *
 * <p><b>A pinned pact that is wrong is named in {@link #KNOWN_WRONG}, by its jar's version</b>, and
 * its interaction is reported as skipped with the reason.
 */
@QuarkusTest
@TestProfile(ContractProfile.class)
@Provider(ConsumerPactVerificationTest.PROVIDER)
@PactSource(ClasspathPactLoader.class)
@TestSecurity(user = "operator", roles = "qits:admin")
class ConsumerPactVerificationTest {

  /** The provider's name in a pact: the repository name, not the application name. */
  static final String PROVIDER = "qits-orchestrator-service";

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  /** A pinned pact interaction this provider cannot honour, and why: the consumer must fix it. */
  record KnownWrong(String consumer, String version, String description, String reason) {

    boolean names(Pact pact, Interaction interaction) {
      return consumer.equals(pact.getConsumer().getName())
          && description.equals(interaction.getDescription())
          && String.valueOf(pact.getSource()).contains("/" + version + "/");
    }
  }

  /**
   * The pinned (or deployed) interactions that are wrong, each by the version of the consumer's
   * jar: the entry is for that version alone, so the bump carrying the consumer's fix verifies the
   * interaction for real. Each is skipped with its reason — never answered by bending the provider
   * to it (the pattern of qits-observability-service's and qits-maintenance-service's).
   *
   * <p>qits-landing-app 2026.1123.90130 pins {@code GET /processes} under {@code the gc process} to
   * exactly ONE process ({@code type} with {@code min}/{@code max} 1, the length of the recording it
   * was generated from). The listing is every registered process, and since ticket qits-2490 that
   * is gc, scan and sbom-check; the landing part of qits-2490 regenerates the pact from the new
   * golden masters.
   */
  static final List<KnownWrong> KNOWN_WRONG =
      List.of(
          new KnownWrong(
              "qits-landing-app",
              "2026.1123.90130",
              "show-orchestrator-process: listProcesses",
              "the row pins the process listing to one entry; the orchestrator lists gc, scan and"
                  + " sbom-check since qits-2490, whose landing part re-records it"));

  @Inject ProviderStates states;

  @TestHTTPResource("/")
  URL base;

  @BeforeEach
  void target(PactVerificationContext context, Pact pact, Interaction interaction) {
    if (context == null) {
      return; // No pact: nothing to target.
    }
    String consumer = pact.getConsumer().getName();
    for (ProviderState state : interaction.getProviderStates()) {
      if (!states.names().contains(state.getName())) {
        fail(
            "Consumer '"
                + consumer
                + "' needs the provider state '"
                + state.getName()
                + "' (interaction '"
                + interaction.getDescription()
                + "'), which qits-orchestrator does not answer for — it answers for "
                + states.names());
      }
    }
    var references = interaction.getComments().get("references");
    for (String key : List.of("qits-call", "qits-trigger")) {
      if (references == null || !references.isObject() || !references.asObject().has(key)) {
        fail(
            "Consumer '"
                + consumer
                + "' interaction '"
                + interaction.getDescription()
                + "' carries no comments.references."
                + key);
      }
    }
    for (KnownWrong wrong : KNOWN_WRONG) {
      if (wrong.names(pact, interaction)) {
        Assumptions.abort(
            consumer + " " + wrong.version() + " '" + interaction.getDescription() + "': "
                + wrong.reason());
      }
    }
    context.setTarget(new HttpTestTarget(base.getHost(), base.getPort()));
  }

  /**
   * A verified {@code start} leaves its run on the executor: nothing after it may find it RUNNING.
   */
  @AfterEach
  void drain() {
    states.awaitNoActiveRun();
  }

  @TestTemplate
  @ExtendWith(PactVerificationInvocationContextProvider.class)
  void consumerPactHolds(PactVerificationContext context) {
    if (context != null) {
      context.verifyInteraction();
    }
  }

  // --- the states: each one line into the registry -------------------------------------------

  @State(ProviderStates.THE_GC_PROCESS)
  Map<String, String> theGcProcess() {
    return states.params(ProviderStates.THE_GC_PROCESS);
  }

  @State(ProviderStates.THE_GC_PROCESS_BEFORE_ITS_FIRST_RUN)
  Map<String, String> theGcProcessBeforeItsFirstRun() {
    return states.params(ProviderStates.THE_GC_PROCESS_BEFORE_ITS_FIRST_RUN);
  }

  @State(ProviderStates.A_FINISHED_GC_DRY_RUN)
  Map<String, String> aFinishedGcDryRun() {
    return states.params(ProviderStates.A_FINISHED_GC_DRY_RUN);
  }

  @State(ProviderStates.A_RUNNING_GC_RUN)
  Map<String, String> aRunningGcRun() {
    return states.params(ProviderStates.A_RUNNING_GC_RUN);
  }

  @State(ProviderStates.NO_ACTIVE_GC_RUN)
  Map<String, String> noActiveGcRun() {
    return states.params(ProviderStates.NO_ACTIVE_GC_RUN);
  }

  @State(ProviderStates.A_REPOSITORY_THE_CLEANUP_CAN_PLAN)
  Map<String, String> aRepositoryTheCleanupCanPlan() {
    return states.params(ProviderStates.A_REPOSITORY_THE_CLEANUP_CAN_PLAN);
  }

  @State(ProviderStates.THE_SCAN_PROCESS)
  Map<String, String> theScanProcess() {
    return states.params(ProviderStates.THE_SCAN_PROCESS);
  }

  @State(ProviderStates.A_FINISHED_SCAN_RUN)
  Map<String, String> aFinishedScanRun() {
    return states.params(ProviderStates.A_FINISHED_SCAN_RUN);
  }

  @State(ProviderStates.NO_ACTIVE_SCAN_RUN)
  Map<String, String> noActiveScanRun() {
    return states.params(ProviderStates.NO_ACTIVE_SCAN_RUN);
  }

  @State(ProviderStates.THE_SBOM_CHECK_PROCESS)
  Map<String, String> theSbomCheckProcess() {
    return states.params(ProviderStates.THE_SBOM_CHECK_PROCESS);
  }

  @State(ProviderStates.A_FINISHED_SBOM_CHECK_RUN)
  Map<String, String> aFinishedSbomCheckRun() {
    return states.params(ProviderStates.A_FINISHED_SBOM_CHECK_RUN);
  }

  @State(ProviderStates.NO_ACTIVE_SBOM_CHECK_RUN)
  Map<String, String> noActiveSbomCheckRun() {
    return states.params(ProviderStates.NO_ACTIVE_SBOM_CHECK_RUN);
  }
}
