package eu.wohlben.qits.orchestrator.stories.support;

import eu.wohlben.qits.orchestrator.api.PackagedSurfaceIT;
import eu.wohlben.qits.servicemock.idp.MockIdp;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <b>One launched qits-platform-orchestrator for the whole story catalogue</b>, and every seam a
 * story moves, declared once.
 *
 * <p>A {@code @TestProfile} is what failsafe launches a process for, so two profiles would be two
 * orchestrators — two boots, two JWKS fetches, two databases, two single-threaded executors, and a
 * diagram whose startup traffic landed in whichever process happened to be running. Every story
 * class therefore names this one, {@code api.TokenValidationBootstrapIT} included: it is a story
 * class like the others and it owns the boot.
 *
 * <p>It extends {@link PackagedSurfaceIT.PackagedUnderTarget} rather than restating it. What a
 * launched qits-platform-orchestrator needs in order to boot at all — the mandatory {@code
 * QITS_RESOURCE_DB_*} triple (the domain jar's datasource expressions have no defaults behind
 * them), and the parking trick that carries it across the two classloaders a test profile is
 * instantiated in — is one answer, written out at length over there, and a second copy would be a
 * second place for it to drift.
 *
 * <h2>Its own database</h2>
 *
 * <p>The catalogue <b>writes</b>: four gc runs and sixty-eight step rows, then reads the history
 * back. Sharing {@code PackagedSurfaceIT}'s database would make either suite's assertions depend on
 * whether the other had run, so the name here is this profile's own and the mechanism is the
 * parent's {@code databaseUrl}.
 *
 * <h2>Every key below is a RUNTIME key</h2>
 *
 * <p>A packaged process takes its configuration as {@code -D} arguments on a jar that was already
 * built, so a build-time key here would be silently ignored and the stories would prove the
 * opposite of what they say.
 *
 * <h2>The seams, and why each one is moved</h2>
 *
 * <ul>
 *   <li><b>{@code qits.auth.machine.required=true}</b> — the gate. The shipped tenant is {@code
 *       quarkus.oidc.tenant-enabled=${qits.auth.machine.required:false}}, so this one key is the
 *       difference between a service that validates machine bearers and one that does not. Every
 *       refusal in this catalogue is a claim only a gate-on packaged run can make, and <b>no other
 *       suite in this repository turns it on</b>.
 *   <li><b>{@code quarkus.oidc.auth-server-url}</b> — where the idp is. Discovery stays off and
 *       {@code jwks-path} stays {@code jwks}, joined onto this URL, so the shipped boot-time fetch
 *       is exercised rather than replaced.
 *   <li><b>the eight target urls</b> — {@link StoryPeers}, replacing the parent's dead port. The
 *       parent points them at a port nothing listens on because its claim is that a failure reaches
 *       a readable row; the claim here is what a run actually DOES, which needs peers that answer.
 *   <li><b>the one named oidc client, {@code qits}, ENABLED</b> — shipped off, because a platform
 *       running its peers open on qits-net behind forward-auth is a supported posture. Turning it on
 *       is what puts this service's own machine credential in the diagram, and it is the half of
 *       {@code PeerClient}'s "two credentials on every call" that a disabled client hides. One
 *       client for all eight peers now (service-client-identity-plan.md, C4): the open calling model
 *       means a single audience, {@code qits-platform}, is good for every one of them.
 * </ul>
 *
 * <h2>The clock stays OFF, and that is a stated coverage gap</h2>
 *
 * <p>{@code quarkus.scheduler.enabled=false} is inherited from the parent and it is load-bearing
 * rather than tidy: {@code GcSchedule} is a cron at 03:00 UTC, and a CI run straddling that minute
 * would start an unattended deletion run out of a test JVM, against peers that now ANSWER. Nor
 * could a recording tell that run's seventeen calls from a story's — the paths are identical — so an
 * arrow would appear or disappear depending on what time the suite ran, which is a {@code
 * networkHash} that never settles.
 *
 * <p>What that costs is stated rather than hidden: <b>no story here covers the SCHEDULED
 * trigger</b>. Every run in the catalogue is {@code manual}, which is the honest trigger for a
 * story — a story is somebody doing something. The clock's own two gates ({@code
 * qits.orchestrator.gc.enabled}, {@code qits.orchestrator.gc.dry-run}), the {@code SKIP} on a
 * concurrent execution and the swallowed {@code RunAlreadyActiveException} have no test in this
 * repository at all, and that is a real gap rather than a story's blind spot.
 */
public class StoryProfile extends PackagedSurfaceIT.PackagedUnderTarget {

  /** Where this catalogue's own run log lives, on this JVM's embedded postgres. */
  private static final String DB_PROPERTY = "qits.test.stories.db-url";

  private static final String DATABASE = "orchestrator_stories_it";

  /**
   * The secret the one named client, {@code qits}, presents with its {@code client_credentials}
   * grant. It is a fixture rather than a credential — {@link StoryPeers} mints for anybody — and it
   * is here because the extension refuses to start a client that has no way to authenticate.
   */
  public static final String CLIENT_SECRET = "story-orchestrator-client-secret";

  /** The eight peers and the target url key each one is read from — {@code PeerTarget}'s constants. */
  private static final Map<String, String> TARGET_URL_KEYS =
      Map.of(
          "artifacts", "qits.orchestrator.targets.artifacts-url",
          "containers", "qits.orchestrator.targets.containers-url",
          "ci", "qits.orchestrator.targets.ci-url",
          "deployments", "qits.orchestrator.targets.deployments-url",
          "projects", "qits.orchestrator.targets.projects-url",
          "workspaces", "qits.orchestrator.targets.workspaces-url",
          "maintenance", "qits.orchestrator.targets.maintenance-url",
          "configuration", "qits.orchestrator.targets.configuration-url");

  @Override
  public Map<String, String> getConfigOverrides() {
    MockIdp idp = MockIdp.ensureStarted();
    String peers = StoryPeers.ensureStarted();

    Map<String, String> overrides = new LinkedHashMap<>(super.getConfigOverrides());
    // A database of this catalogue's own — the parent's parking trick, this profile's name.
    overrides.put("QITS_RESOURCE_DB_URL", databaseUrl(DB_PROPERTY, DATABASE));

    // The gate, and where the keys it validates against come from.
    overrides.put("qits.auth.machine.required", "true");
    overrides.put("quarkus.oidc.auth-server-url", idp.baseUrl());

    // Where each peer is — one stub answering as all eight, told apart by path prefix.
    TARGET_URL_KEYS.forEach((peer, key) -> overrides.put(key, peers));

    // …and the ONE credential this service presents to every one of them (service-client-identity-
    // plan.md, C4): a token is cut for the platform now, not for one peer, so one client and one
    // audience — qits-platform, what a single-environment platform's own receivers accept — serve
    // all eight calls.
    overrides.put("quarkus.oidc-client.qits.client-enabled", "true");
    overrides.put("quarkus.oidc-client.qits.auth-server-url", peers + "/idp");
    overrides.put("quarkus.oidc-client.qits.credentials.secret", CLIENT_SECRET);
    overrides.put("quarkus.oidc-client.qits.grant-options.client.audience", "qits-platform");
    return overrides;
  }
}
