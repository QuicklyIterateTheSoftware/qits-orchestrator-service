package eu.wohlben.qits.orchestrator.bus;

import eu.wohlben.qits.eventstream.control.EventEnvelope;
import eu.wohlben.qits.eventstream.control.EventFrame;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * What the event bus binds to and from JSON, told to native-image. No code, no bean, nothing at
 * runtime: the annotation is the entire content, and this class exists so that the annotation has
 * somewhere to live that can say why.
 *
 * <p><b>Why nothing registers these automatically.</b> Quarkus registers reflection for the classes
 * <em>it</em> knows are serialized — a REST resource's parameters and return types, whatever the CDI
 * {@code ObjectMapper} is handed. {@code CanonicalJson} builds its <b>own</b> {@code ObjectMapper} by
 * hand, deliberately and permanently, because the canonical form is a wire contract another service
 * compares byte-for-byte and must not be downstream of any application's customizer. Correct, and
 * this is the price: to the build step scanning for what needs reflecting on, that mapper and
 * everything it touches are invisible. Do not "fix" a recurrence by injecting the CDI mapper.
 *
 * <p>This is the same family as {@code api/ApiWireReflection} and there for the same reason — a
 * failure that is invisible to every JVM test by construction, and fatal to the binary. qits-ci paid
 * for the lesson on a deployed one: every green build's publish died inside {@code CanonicalJson}
 * with Jackson's "no serializer found … you may need to configure reflection", JVM suite green
 * throughout.
 *
 * <p><b>Why these five.</b> {@link EventFrame} is what arrives on {@code /events/stream} and what
 * the catch-up sweep reads out of the log. {@link DeploymentActiveListener.DeploymentActivePayload}
 * is the wire type this binary actually has to deserialize: qits-deployments' vocabulary jar is not
 * on this classpath (the platform's Maven registry serves nothing under that coordinate), so the
 * local record is the bound type and not {@code DeploymentActive} itself. An absent line here is a
 * native binary that subscribes to every deployment and cannot read one — no run would ever be
 * triggered again, silently, with the JVM suite green.
 *
 * <p>{@link ProcessRunChanged} and {@link ProcessRunStepChanged} are what this service PUBLISHES
 * (qits-2472), and {@code CanonicalJson.payload} serializes each through that same hand-built
 * mapper. An absent line is a native binary whose every run announcement fails at publish — no
 * crash, the run itself untouched, and every page following a run on the bus waiting for an event
 * that never comes. {@link EventEnvelope} is the PUT body they travel in; it was listed before
 * anything here published, for the reason qits-projects listed it before it had a publisher.
 *
 * <p><b>And why a mix-in by name.</b> {@code CanonicalJson$QitsEventMixin} keeps {@code QitsEvent}'s
 * declared methods — {@code eventId} above all — out of a payload, and Jackson finds its {@code
 * @JsonIgnore}s by calling {@code getDeclaredMethods()} on it, which is reflection like any other.
 * qits-ci measured what leaving it out costs: no crash, no log, {@code eventId} simply present in a
 * payload supposed to carry no identity at all. It is a string because the class is private and
 * stays private.
 *
 * <p>All of this is in {@code service/} because {@code service/} is the deployable, and the
 * deployable is what gets built into an image and therefore what tells the builder about itself.
 */
@RegisterForReflection(
    targets = {
      DeploymentActiveListener.DeploymentActivePayload.class,
      EventEnvelope.class,
      EventFrame.class,
      ProcessRunChanged.class,
      ProcessRunStepChanged.class
    },
    classNames = "eu.wohlben.qits.eventstream.control.CanonicalJson$QitsEventMixin")
public final class EventWireReflection {

  private EventWireReflection() {}
}
