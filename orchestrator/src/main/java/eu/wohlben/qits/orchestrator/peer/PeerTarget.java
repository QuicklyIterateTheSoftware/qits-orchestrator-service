package eu.wohlben.qits.orchestrator.peer;

/**
 * The nine peers, by their wire name.
 *
 * <p>Constants rather than an enum because the same string is two things at once: the value a step
 * reports as its {@code target}, and the middle of the config key {@code
 * qits.orchestrator.targets.<name>-url}. An enum would have to spell the mapping out twice; a
 * string spells it once.
 *
 * <p>Before the epic qits-540 dossier's 'Plan (as of 2026-09-13)', C4, this string was also the
 * name of a dedicated oidc client — one per peer, because a token was cut FOR one service. {@code
 * PeerTokens} now mints from one named client, {@code qits}, for every peer alike.
 */
public final class PeerTarget {

  /** qits-artifacts — the registry's own GC engine, plan and sweep. */
  public static final String ARTIFACTS = "artifacts";

  /** qits-containers — the platform's docker socket: images, volumes and buildkit cache. */
  public static final String CONTAINERS = "containers";

  /** qits-ci — the daemon-binary pin. */
  public static final String CI = "ci";

  /** qits-platform-deployments — the image shas a restart or a rollback would pull. */
  public static final String DEPLOYMENTS = "deployments";

  /** qits-projects — the repository catalogue the branch sweep runs over. */
  public static final String PROJECTS = "projects";

  /** qits-workspaces — branch semantics and the merged-branch sweep. */
  public static final String WORKSPACES = "workspaces";

  /** qits-platform-maintenance — the dependency pins every repository's main still references. */
  public static final String MAINTENANCE = "maintenance";

  /** qits-configuration — the container images a workspace, editor or agent launch would pull. */
  public static final String CONFIGURATION = "configuration";

  /**
   * qits-idp — the service-client store, swept against the idp-client claims qits-deployments
   * holds (ticket qits-878). The same {@code qits} bearer as every other peer: qits-idp mints
   * {@code qits-platform} on every token, so no second client and no second audience.
   */
  public static final String IDP = "idp";

  private PeerTarget() {}
}
