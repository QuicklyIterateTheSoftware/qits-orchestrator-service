package eu.wohlben.qits.orchestrator.contracts;

import static org.junit.jupiter.api.Assertions.fail;

import au.com.dius.pact.core.model.Interaction;
import au.com.dius.pact.core.model.Pact;
import au.com.dius.pact.core.model.ProviderState;
import au.com.dius.pact.provider.junit5.HttpTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.IgnoreNoPactsToVerify;
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
 * <p><b>No consumer pins a pact yet.</b> The caller today is qits-orchestrator-frontend's {@code
 * orchestrator-api.ts}, which writes none. So this class carries {@link IgnoreNoPactsToVerify} and
 * {@link ClasspathPactLoader#REQUIRED} is false: today it verifies nothing, and it is where the
 * first consumer's pin lands. When one is pinned, drop the annotation and set {@code REQUIRED} to
 * true, so a classpath that lost it fails rather than verifying nothing.
 *
 * <p>Each interaction runs against this {@code @QuarkusTest} application over real HTTP, as an
 * operator holding {@code qits:admin} ({@code @TestSecurity}), with the peers faked at {@code
 * FakePeers} and the run log fresh ({@link ContractProfile}, shared with {@link
 * GoldenMasterRecordingTest}). Every {@code @State} method delegates to {@link ProviderStates};
 * {@link #target} fails an unknown state, and an interaction without {@code
 * comments.references.qits-call} or {@code qits-trigger}. {@link #drain} waits out the run a
 * verified {@code start} leaves on the executor, as the recording does.
 */
@QuarkusTest
@TestProfile(ContractProfile.class)
@Provider(ConsumerPactVerificationTest.PROVIDER)
@PactSource(ClasspathPactLoader.class)
@IgnoreNoPactsToVerify
@TestSecurity(user = "operator", roles = "qits:admin")
class ConsumerPactVerificationTest {

  /** The provider's name in a pact: the repository name, not the application name. */
  static final String PROVIDER = "qits-orchestrator-service";

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

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
    context.setTarget(new HttpTestTarget(base.getHost(), base.getPort()));
  }

  /**
   * A verified {@code start} leaves its run on the executor: nothing after it may find it RUNNING.
   */
  @AfterEach
  void drain() {
    states.awaitNoActiveGcRun();
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

  @State(ProviderStates.A_FINISHED_GC_DRY_RUN)
  Map<String, String> aFinishedGcDryRun() {
    return states.params(ProviderStates.A_FINISHED_GC_DRY_RUN);
  }

  @State(ProviderStates.NO_ACTIVE_GC_RUN)
  Map<String, String> noActiveGcRun() {
    return states.params(ProviderStates.NO_ACTIVE_GC_RUN);
  }
}
