package eu.wohlben.qits.orchestrator.process.gc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.orchestrator.peer.ContractPeers;
import eu.wohlben.qits.orchestrator.peer.FakePeers;
import eu.wohlben.qits.orchestrator.peer.PeerClient;
import eu.wohlben.qits.orchestrator.peer.PeerTarget;
import eu.wohlben.qits.orchestrator.process.RunContext;
import eu.wohlben.qits.orchestrator.process.StepDefinition;
import eu.wohlben.qits.orchestrator.process.StepResult;
import eu.wohlben.qits.orchestrator.process.TechnicalProcess;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckConfig;
import eu.wohlben.qits.orchestrator.process.sbomcheck.SbomCheckProcess;
import eu.wohlben.qits.orchestrator.process.scan.ScanConfig;
import eu.wohlben.qits.orchestrator.process.scan.ScanProcess;
import eu.wohlben.qits.pact.consumer.GoldenInteraction;
import eu.wohlben.qits.pact.consumer.GoldenMasters;
import eu.wohlben.qits.pact.consumer.GoldenMasters.Operation;
import eu.wohlben.qits.pact.consumer.ProviderStateMissing;
import eu.wohlben.qits.pact.consumer.Trigger;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * <b>What the scan and SBOM check processes ask qits-maintenance, and what they read</b> (ticket
 * qits-2490) — the rows {@link GcContract#pact} adds to this service's one qits-maintenance pact
 * beside the gc rows, and the table {@code MaintenanceProcessesConsumerPactTest} and {@code
 * MaintenanceProcessesCallCoverageTest} are built from. Here rather than in {@link GcContract}
 * because its rows are gc steps; the pact is per peer, not per process.
 *
 * <p><b>One row per call.</b> The scan process makes two calls four times over — {@code
 * internal.*} and {@code external.*} differ only in the scope — so its rows are the internal pair.
 *
 * <p><b>A row is LIVE only when the recording is the request the step sends</b> ({@link #live}).
 * {@code requestScan} is recorded under {@value #A_FINISHED_SCAN} with a body naming ONE repository
 * ({@code {"scope":"INTERNAL","repository":"{repoName}"}}), and this process scans the whole
 * catalogue ({@code {"scope":"INTERNAL"}}): no honest run of the step sends the recorded request,
 * so the row is left out of the pact file and reported skipped with what it needs — the same
 * pending shape as {@link GcContract#PENDING}. A recording of the body the step sends switches it
 * on (then the committed pact fails its comparison until rewritten with {@code
 * -Dgolden.update=true}).
 */
final class MaintenanceProcessesContract {

  private static final ObjectMapper JSON = new ObjectMapper();

  static final GoldenMasters MAINTENANCE = GcContract.MAINTENANCE;

  /** The nightly triggers each process's calls are reached from. */
  static final Trigger SCAN_NIGHTLY = Trigger.schedule("ScanSchedule.onSchedule");

  static final Trigger SBOM_CHECK_NIGHTLY = Trigger.schedule("SbomCheckSchedule.onSchedule");

  /** qits-maintenance's states the rows are recorded in. */
  static final String A_FINISHED_SCAN = "a finished scan";

  static final String A_STORED_SBOM_CHECK_REPORT = "a stored SBOM check report";

  /**
   * One call.
   *
   * @param kind the process that makes it
   * @param stepId the step that makes it
   * @param body the request body the step sends, or null for none — what {@link #live} compares
   *     with the recording
   * @param needs what the recording must be before the row can be live, when it is not
   */
  record Row(
      String kind, String stepId, GoldenInteraction interaction, String body, String needs) {}

  static final List<Row> ROWS =
      List.of(
          // internal.request: the 202 {id} the follow reads. Pending: see the class comment.
          new Row(
              ScanProcess.KIND,
              ScanProcess.INTERNAL_REQUEST,
              GoldenInteraction.of(SCAN_NIGHTLY, A_FINISHED_SCAN, "requestScan").consumes("id"),
              "{\"scope\":\"INTERNAL\"}",
              "a requestScan recording whose body is {\"scope\":\"INTERNAL\"} with no repository —"
                  + " the scan process scans the whole catalogue, and the recording under '"
                  + A_FINISHED_SCAN
                  + "' names one repository"),
          // internal.follow: a finished scan, read for its scope and status. `message` is read too,
          // but only from a FAILED scan, and the recording is a SUCCEEDED one whose message is null.
          new Row(
              ScanProcess.KIND,
              ScanProcess.INTERNAL_FOLLOW,
              GoldenInteraction.of(SCAN_NIGHTLY, A_FINISHED_SCAN, "getScan")
                  .consumes("scope", "status"),
              null,
              null),
          // check: the report the door stored, read for its counts. The arrays are read for their
          // size alone; no element field is this service's to bind.
          new Row(
              SbomCheckProcess.KIND,
              SbomCheckProcess.CHECK,
              GoldenInteraction.of(SBOM_CHECK_NIGHTLY, A_STORED_SBOM_CHECK_REPORT, "runSbomCheck")
                  .consumes("filed", "entries[]", "warnings[]", "tickets[]")
                  .anyLength("entries"),
              null,
              null));

  /**
   * Whether {@code row} is in the pact: its operation is recorded, and recorded with the body the
   * step sends (none, or the same JSON).
   */
  static boolean live(Row row) {
    GoldenInteraction interaction = row.interaction();
    if (!MAINTENANCE.recorded(interaction.state(), interaction.operationId())) {
      return false;
    }
    JsonNode recorded = recorded(row).body();
    try {
      JsonNode sent = row.body() == null ? null : JSON.readTree(row.body());
      return Objects.equals(recorded, sent);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Why {@code row} is not live: the state is missing, or the recording is another request. */
  static String needs(Row row) {
    GoldenInteraction interaction = row.interaction();
    if (!MAINTENANCE.recorded(interaction.state(), interaction.operationId())) {
      return ProviderStateMissing.message(
              MAINTENANCE.repository(), interaction.state(), interaction.operationId())
          + (row.needs() == null ? "" : " — needs: " + row.needs());
    }
    return MAINTENANCE.repository()
        + " records "
        + interaction.operationId()
        + " under '"
        + interaction.state()
        + "' with the body "
        + recorded(row).body()
        + ", the step sends "
        + row.body()
        + " — needs: "
        + row.needs();
  }

  static Operation recorded(Row row) {
    return MAINTENANCE.operation(row.interaction().state(), row.interaction().operationId());
  }

  /** The row's call as {@code "<target> <METHOD> <path>"}, the path as the recording's example. */
  static String call(Row row) {
    Operation op = recorded(row);
    return PeerTarget.MAINTENANCE + " " + op.method() + " " + op.examplePath();
  }

  // --- the processes, as shipped ------------------------------------------------------------------

  /** The shipped settings, read from the file the jar ships them in. */
  static Properties shipped() {
    Properties shipped = new Properties();
    try (InputStream in =
        MaintenanceProcessesContract.class.getResourceAsStream(
            "/META-INF/microprofile-config.properties")) {
      shipped.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return shipped;
  }

  /**
   * The scan process as shipped, outside CDI — but with a wait that does not wait: a row answers
   * a finished scan at once, and a coverage pass must never hold the suite for ten seconds.
   */
  static ScanProcess scan() {
    Properties shipped = shipped();
    return ScanProcess.of(
        ScanConfig.of(
            Boolean.parseBoolean(shipped.getProperty("qits.orchestrator.scan.enabled")),
            Duration.parse(shipped.getProperty("qits.orchestrator.scan.poll-interval")),
            Duration.parse(shipped.getProperty("qits.orchestrator.scan.follow-timeout"))),
        duration -> {},
        Clock.systemUTC());
  }

  /** The SBOM check process as shipped, outside CDI. */
  static SbomCheckProcess sbomCheck() {
    Properties shipped = shipped();
    return SbomCheckProcess.of(
        SbomCheckConfig.of(
            Boolean.parseBoolean(shipped.getProperty("qits.orchestrator.sbom-check.enabled")),
            Duration.parse(shipped.getProperty("qits.orchestrator.sbom-check.call-timeout"))));
  }

  static TechnicalProcess process(String kind) {
    return switch (kind) {
      case ScanProcess.KIND -> scan();
      case SbomCheckProcess.KIND -> sbomCheck();
      default -> throw new IllegalArgumentException("no process " + kind);
    };
  }

  static StepDefinition step(TechnicalProcess process, String stepId) {
    return process.steps().stream()
        .filter(s -> s.id().equals(stepId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no " + process.kind() + " step " + stepId));
  }

  /**
   * {@code row}'s step calling {@code peers}, arranged as its run would have it: a follow step's
   * request step has answered the scan id the recording reads.
   */
  static StepResult run(Row row, PeerClient peers) {
    RunContext context = new RunContext(UUID.randomUUID(), false, peers);
    if (row.stepId().equals(ScanProcess.INTERNAL_FOLLOW)) {
      String id = MAINTENANCE.params(row.interaction().state()).get("scanId");
      JsonNode answer = JSON.createObjectNode().put("id", id);
      context.record(ScanProcess.INTERNAL_REQUEST, answer, answer.toString());
    }
    return step(process(row.kind()), row.stepId()).body().run(context);
  }

  /** The step run against the provider's WHOLE recorded answer, through a fake. */
  static StepResult againstRecording(Row row) {
    FakePeers fake = ContractPeers.fake();
    fake.answer(
        recorded(row).examplePath(),
        FakePeers.Scripted.status(
            recorded(row).status(),
            MAINTENANCE.body(row.interaction().state(), row.interaction().operationId())));
    return run(row, fake);
  }

  /** The step run against the recorded answer cut to what the row {@code consumes}. */
  static StepResult againstConsumed(Row row) {
    JsonNode whole = MAINTENANCE.json(row.interaction().state(), row.interaction().operationId());
    FakePeers fake = ContractPeers.fake();
    fake.answer(
        recorded(row).examplePath(),
        FakePeers.Scripted.status(
            recorded(row).status(),
            GcContract.cutTo(whole, row.interaction().consumes()).toString()));
    return run(row, fake);
  }

  /** The recorded answer of every row, by path — what a coverage pass scripts its fake with. */
  static Map<String, String> recordedAnswers() {
    Map<String, String> answers = new java.util.TreeMap<>();
    for (Row row : ROWS) {
      if (MAINTENANCE.recorded(row.interaction().state(), row.interaction().operationId())) {
        answers.put(
            recorded(row).examplePath(),
            MAINTENANCE.body(row.interaction().state(), row.interaction().operationId()));
      }
    }
    return answers;
  }

  private MaintenanceProcessesContract() {}
}
