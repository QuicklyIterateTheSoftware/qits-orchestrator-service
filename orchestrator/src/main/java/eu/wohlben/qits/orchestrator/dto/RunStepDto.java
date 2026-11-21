package eu.wohlben.qits.orchestrator.dto;

import java.time.Instant;
import java.util.List;

/**
 * One step of one RUN, as {@code GET /runs/{id}} serves it in its {@code steps}, and {@code GET
 * /runs/{id}/steps/{stepId}} serves it alone: the definition's fields plus what happened.
 *
 * <p><b>{@code response} is a STRING carrying the peer's JSON text</b>, bounded at 1 MiB with a
 * marker appended past it. It is served as the bytes that arrived rather than as an embedded
 * object, for two reasons: a truncated document is not parseable JSON and would have nowhere to go
 * in an object-typed field, and a re-serialisation would be this service's opinion about another
 * service's answer. A client that wants the tree parses the string and copes with a body that does
 * not parse.
 *
 * <p><b>{@code responseBytes} is there whether or not the bodies are</b> (ticket qits-2359). {@code
 * GET /runs/{id}?bodies=false} serves every step with {@code request.body} and {@code response}
 * null, so a client polling a RUNNING run every two seconds does not pull a megabyte of stored
 * answers each time; the size tells it what a {@code GET /runs/{id}/steps/{stepId}} would bring
 * back before it asks.
 *
 * @param id the step id from the definition, NOT the storage row's uuid
 * @param name the human line
 * @param target the peer
 * @param dependsOn the step ids it waited for, as the definition had them when the run opened
 * @param status PENDING, RUNNING, SUCCEEDED, FAILED or SKIPPED
 * @param startedAt when it began, or null if it never did
 * @param finishedAt when it ended, or null
 * @param httpStatus the peer's status code, or null when there was none
 * @param request what was sent, or null for a step that sent nothing
 * @param response the peer's body as text, or null — null too on a body-less read
 * @param responseBytes the stored response text's size in UTF-8 bytes (the truncation marker
 *     included when there is one), or null when nothing was stored; present on a body-less read
 * @param error why it failed or why it was skipped, or null
 * @param summary the one human line, or null
 */
public record RunStepDto(
    String id,
    String name,
    String target,
    List<String> dependsOn,
    String status,
    Instant startedAt,
    Instant finishedAt,
    Integer httpStatus,
    RequestDto request,
    String response,
    Integer responseBytes,
    String error,
    String summary) {}
