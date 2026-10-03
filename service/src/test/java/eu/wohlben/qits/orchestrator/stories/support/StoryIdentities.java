package eu.wohlben.qits.orchestrator.stories.support;

import eu.wohlben.qits.servicemock.idp.MockIdp;
import io.restassured.specification.RequestSpecification;

/**
 * The two identity tracks this service accepts, one helper each — and the one thing that makes this
 * service different from most of its siblings: <b>both tracks open every route</b>.
 *
 * <ul>
 *   <li><b>{@code qits:admin} is a PERSON's</b>, and it arrives only as the {@code X-Qits-User} /
 *       {@code X-Qits-Roles} pair the platform edge asserts for an authenticated admin session.
 *   <li><b>{@code qits:system} is a MACHINE's</b>, and it arrives only in an idp-minted bearer:
 *       qits-idp copies a client's granted roles into the token's {@code groups} claim and
 *       quarkus-oidc reads it as roles with no configuration at all.
 * </ul>
 *
 * <p>Every route here is {@code @RolesAllowed({"qits:admin", "qits:system"})} and that is a
 * decision rather than laziness: an operator presses Run now in a browser, and a machine may post
 * the same run. A machine-only guard would lock the operator out of the button this service exists
 * to offer. So there is no story here about one track being refused the other's door — there is no
 * such door. What there IS a story about is the third role: a real platform role this service never
 * names, which authenticates perfectly and covers nothing.
 *
 * <p><b>There is no anonymous route and there must never be one.</b> The write surface starts
 * deletions on six other services.
 *
 * <p><b>The synthetic {@code %test} dev user is not available here, and that is the point.</b>
 * qits-auth-core's dev identity holds every platform role and is {@code LaunchMode}-guarded, while
 * a launched artifact runs in {@code NORMAL} mode — so an anonymous request really is anonymous and
 * the credentials below are the only thing opening these doors. {@code ProcessApiTest} runs under
 * the dev user and can make no refusal claim at all.
 *
 * <p>Minting is local crypto against the keypair {@link MockIdp} parked at startup: it makes no
 * request to the mock, which is why no story's diagram carries an arrow for GETTING a token. The
 * one token that <i>is</i> fetched over the wire is this service's own, outbound — see {@link
 * StoryPeers}.
 */
public final class StoryIdentities {

  /**
   * The audience this service enforces, and it is the PLATFORM's rather than a name of its own:
   * qits-idp puts {@code qits-platform} on every token it mints, so every caller on this
   * platform addresses this service by it and the roles decide the rest. {@code
   * quarkus.oidc.token.audience=qits-platform} is spelled as a literal in {@code
   * application.properties} and so is this, so the audience under test is the shipped one.
   */
  public static final String AUDIENCE = "qits-platform";

  /** The machine role: a bearer's, and one of the two every route names. */
  public static final String MACHINE_ROLE = "qits:system";

  /** The person's role: a forwarded header's, and the other of the two. */
  public static final String HUMAN_ROLE = "qits:admin";

  /** An audience no token of this platform carries — the credential that never becomes an identity. */
  public static final String OFF_PLATFORM_AUDIENCE = "some-other-platform";

  /** A real platform role this service names nowhere — the 403 that is not a 401. */
  public static final String UNPRIVILEGED_ROLE = "qits:reader";

  /** The header the edge names the logged-in person in. */
  public static final String USER_HEADER = "X-Qits-User";

  /** The header the edge asserts that person's roles in, comma-separated. */
  public static final String ROLES_HEADER = "X-Qits-Roles";

  // --- how a diagram names each initiator ------------------------------------------------------

  /** The person who presses Run now, and who reads what a run did afterwards. */
  public static final String OPERATOR = "an operator";

  /** A machine holding this service's audience — the other half of "a machine may post the run". */
  public static final String MACHINE = "a platform machine";

  /** Nobody at all: no bearer, no forwarded pair. */
  public static final String ANONYMOUS = "an unauthenticated caller";

  /** A credential that looks right and is not: an audience from outside this platform. */
  public static final String IMPOSTOR = "an impostor";

  /** A real caller, correctly authenticated, holding a role this service never names. */
  public static final String WRONG_ROLE = "a caller with the wrong role";

  /** The account the person stories log in as. Authored, so it survives label scrubbing. */
  public static final String OPERATOR_ACCOUNT = "story-operator";

  private StoryIdentities() {}

  /**
   * A machine peer's bearer.
   *
   * <p>Minted fresh per call rather than cached: a token is a credential, and a helper that handed
   * the same string to two stories would make {@link
   * eu.wohlben.qits.userflows.report.ReportAssertions#assertNotLeaked} a weaker claim than it reads
   * as.
   */
  public static String machineToken(String subject) {
    return token(subject, AUDIENCE, MACHINE_ROLE);
  }

  /**
   * A token addressed somewhere that is not this platform. It has to be minted OUTSIDE the platform
   * audience to be a refusal at all: a sibling's bearer carries {@code qits-platform} like every
   * other token qits-idp mints, so it is admitted here and its roles decide what it may do.
   */
  public static String offPlatformToken(String subject) {
    return token(subject, OFF_PLATFORM_AUDIENCE, MACHINE_ROLE);
  }

  /** Addressed here, signed correctly, carrying a role no route names: authenticated, and covered by nothing. */
  public static String unprivilegedToken(String subject) {
    return token(subject, AUDIENCE, UNPRIVILEGED_ROLE);
  }

  private static String token(String subject, String audience, String role) {
    return MockIdp.attach().token().subject(subject).audience(audience).groups(role).mint();
  }

  /** {@code given()} with one machine's bearer on it. */
  public static RequestSpecification bearer(RequestSpecification request, String token) {
    return request.header("Authorization", "Bearer " + token);
  }

  /** {@code given()} with the pair the edge asserts for a logged-in admin session. */
  public static RequestSpecification person(RequestSpecification request, String user) {
    return person(request, user, HUMAN_ROLE);
  }

  /** …and the same pair for a session holding some other role, which is how a 403 is asked for. */
  public static RequestSpecification person(
      RequestSpecification request, String user, String roles) {
    return request.header(USER_HEADER, user).header(ROLES_HEADER, roles);
  }
}
