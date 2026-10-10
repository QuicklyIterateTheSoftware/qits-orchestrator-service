package eu.wohlben.qits.orchestrator.peer;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * What a peer said, or why it said nothing.
 *
 * <p><b>A transport failure is an answer here, not an exception.</b> A step that cannot reach its
 * peer has to be recorded exactly like a step that reached it and was refused — same row, same
 * card, a sentence in {@code error} either way. Throwing would put half the failures on one code
 * path and half on another, and the caller has nothing different to do with them.
 *
 * @param httpStatus the peer's status code, or null when the call never got one
 * @param body the response body as text, bounded and marked by {@link PeerClient}
 * @param json the same body parsed, or null when it was not JSON — a step reads its summary from
 *     this and copes with null rather than assuming a shape
 * @param error the transport failure or the parse failure, or null when the call completed
 * @param truncated whether the peer's answer was larger than {@link PeerClient#PARSE_LIMIT_BYTES},
 *     so it was not read in full and {@code json} is null. A step that judges by the WHOLE answer —
 *     every pin read — must treat this as a failed read, never as an empty one (ticket qits-1175).
 */
public record PeerAnswer(
    Integer httpStatus, String body, JsonNode json, String error, boolean truncated) {

  /** An answer that was read in full, or one that never arrived. */
  public PeerAnswer(Integer httpStatus, String body, JsonNode json, String error) {
    this(httpStatus, body, json, error, false);
  }

  /** Whether the peer answered 2xx. Anything else is a FAILED step. */
  public boolean ok() {
    return httpStatus != null && httpStatus >= 200 && httpStatus < 300;
  }
}
