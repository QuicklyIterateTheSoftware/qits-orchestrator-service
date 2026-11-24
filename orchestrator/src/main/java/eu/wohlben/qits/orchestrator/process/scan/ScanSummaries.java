package eu.wohlben.qits.orchestrator.process.scan;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The scan steps' one-line summaries, read out of qits-maintenance's answers and computing nothing.
 */
final class ScanSummaries {

  private ScanSummaries() {}

  /** {@code POST /scans}' 202: {@code {id}}. */
  static String requested(JsonNode answer) {
    return "queued scan " + answer.path("id").asText();
  }

  /** {@code GET /scans/{id}} once it ended: {@code {id, scope, status, …}}. */
  static String finished(JsonNode scan) {
    return scan.path("scope").asText() + " scan " + scan.path("status").asText();
  }
}
