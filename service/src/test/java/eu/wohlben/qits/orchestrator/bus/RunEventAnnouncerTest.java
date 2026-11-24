package eu.wohlben.qits.orchestrator.bus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import eu.wohlben.qits.eventstream.QitsEvent;
import eu.wohlben.qits.eventstream.control.CanonicalJson;
import eu.wohlben.qits.eventstream.control.EventEnvelope;
import eu.wohlben.qits.orchestrator.run.RunAnnouncer.RunChange;
import eu.wohlben.qits.orchestrator.run.RunAnnouncer.StepChange;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import eu.wohlben.qits.orchestrator.run.RunTrigger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The two run events on the wire (qits-2472): one wire name each, the domain's enums spelled the
 * way the API spells them, {@code changedAt} as the envelope's {@code occurredAt}, and a payload
 * that carries no identity of its own.
 *
 * <p>A plain unit test over a subclass that records instead of publishing — what reaches the bus is
 * the library's to prove; what reaches {@code publish} is this repository's.
 */
class RunEventAnnouncerTest {

  private static final UUID RUN = UUID.fromString("0c4b1b7e-3a52-4e0a-9f6e-6d2f8f3b9a10");

  private static final Instant STARTED = Instant.parse("2026-11-24T03:00:00Z");

  private static final Instant FINISHED = Instant.parse("2026-11-24T03:07:12Z");

  /** The adapter with its one exit replaced: what it would have published, in order. */
  private static final class Recording extends RunEventAnnouncer {

    final List<QitsEvent> published = new ArrayList<>();

    @Override
    protected void publish(QitsEvent event) {
      published.add(event);
    }
  }

  private static JsonNode payload(QitsEvent event) {
    return CanonicalJson.parse(CanonicalJson.payload(event));
  }

  private static Set<String> fields(JsonNode node) {
    Set<String> names = new TreeSet<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }

  @Test
  void aRunTransitionIsAProcessRunChangedCarryingTheRunsFields() {
    Recording announcer = new Recording();
    announcer.runChanged(
        new RunChange(
            "gc", RUN, RunStatus.FAILED, RunTrigger.EVENT, false, STARTED, FINISHED, FINISHED));

    ProcessRunChanged event = assertInstanceOf(ProcessRunChanged.class, announcer.published.get(0));
    assertEquals("ProcessRunChanged", event.signature());
    assertEquals("ProcessRunChanged", EventEnvelope.of(event).name());
    assertEquals(FINISHED, event.occurredAt());
    assertEquals(FINISHED, EventEnvelope.of(event).occurredAt());

    JsonNode payload = payload(event);
    assertEquals(
        Set.of(
            "kind",
            "runId",
            "status",
            "trigger",
            "dryRun",
            "startedAt",
            "finishedAt",
            "changedAt"),
        fields(payload));
    assertEquals("gc", payload.get("kind").asText());
    assertEquals(RUN.toString(), payload.get("runId").asText());
    assertEquals("FAILED", payload.get("status").asText());
    // The wire spelling the run list serves, not the constant.
    assertEquals("event", payload.get("trigger").asText());
    assertFalse(payload.get("dryRun").asBoolean());
    assertFalse(payload.get("startedAt").isNull());
    assertFalse(payload.get("finishedAt").isNull());
  }

  @Test
  void anOpeningRunCarriesNoFinishTime() {
    Recording announcer = new Recording();
    announcer.runChanged(
        new RunChange(
            "gc", RUN, RunStatus.RUNNING, RunTrigger.MANUAL, true, STARTED, null, STARTED));

    ProcessRunChanged event = (ProcessRunChanged) announcer.published.get(0);
    assertEquals(STARTED, event.occurredAt());
    JsonNode payload = payload(event);
    assertEquals("RUNNING", payload.get("status").asText());
    assertEquals("manual", payload.get("trigger").asText());
    assertTrue(payload.get("dryRun").asBoolean());
    assertTrue(payload.path("finishedAt").isMissingNode() || payload.get("finishedAt").isNull());
  }

  @Test
  void aStepTransitionIsAProcessRunStepChangedWithNoBodies() {
    Recording announcer = new Recording();
    announcer.stepChanged(new StepChange("gc", RUN, "artifacts.plan", RunStatus.SKIPPED, FINISHED));

    ProcessRunStepChanged event =
        assertInstanceOf(ProcessRunStepChanged.class, announcer.published.get(0));
    assertEquals("ProcessRunStepChanged", event.signature());
    assertEquals("ProcessRunStepChanged", EventEnvelope.of(event).name());
    assertEquals(FINISHED, event.occurredAt());

    JsonNode payload = payload(event);
    assertEquals(Set.of("kind", "runId", "stepId", "status", "changedAt"), fields(payload));
    assertEquals("gc", payload.get("kind").asText());
    assertEquals(RUN.toString(), payload.get("runId").asText());
    assertEquals("artifacts.plan", payload.get("stepId").asText());
    assertEquals("SKIPPED", payload.get("status").asText());
  }

  /** Each event is its own: a fresh id per event, and none of it in the payload. */
  @Test
  void everyEventHasItsOwnIdAndThePayloadCarriesNone() {
    Recording announcer = new Recording();
    StepChange change = new StepChange("gc", RUN, "usage.before", RunStatus.RUNNING, STARTED);
    announcer.stepChanged(change);
    announcer.stepChanged(change);

    assertFalse(
        announcer.published.get(0).eventId().equals(announcer.published.get(1).eventId()),
        "two publishes are two events");
    for (QitsEvent event : announcer.published) {
      assertTrue(payload(event).path("eventId").isMissingNode(), CanonicalJson.payload(event));
    }
    assertTrue(
        payload(RunEventAnnouncer.event(
                new RunChange(
                    "gc", RUN, RunStatus.RUNNING, RunTrigger.MANUAL, false, STARTED, null,
                    STARTED)))
            .path("eventId")
            .isMissingNode());
  }
}
