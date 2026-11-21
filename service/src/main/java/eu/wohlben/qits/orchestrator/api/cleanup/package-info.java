/**
 * The wire shape qits-artifacts answers the artifacts cleanup listing with (qits-2329; the plan's
 * and the sweep's went with their doors, qits-2477).
 *
 * <p><b>Schema only.</b> {@code ArtifactsCleanupController} hands qits-artifacts' answer on
 * verbatim — its status and its text, never re-bound into a type here — so nothing in this package
 * is ever instantiated or serialised by this service, and none of it needs registering for
 * reflection. The records exist because a door that hands text on verbatim documents as {@code
 * schema: {}}, which a consumer cannot generate a typed client from: they are the shapes of
 * qits-artifacts' {@code eu.wohlben.qits.artifacts.gc.dto} records, mirrored field for field and
 * named the same, documented here because this door is where those consumers read them.
 *
 * <p>When qits-artifacts changes one of those records, change its mirror here in the same breath;
 * the golden masters of {@code a repository the cleanup can plan} carry qits-artifacts' recorded
 * answers and show the drift.
 */
package eu.wohlben.qits.orchestrator.api.cleanup;
