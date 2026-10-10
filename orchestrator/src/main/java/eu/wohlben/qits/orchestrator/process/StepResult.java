package eu.wohlben.qits.orchestrator.process;

import eu.wohlben.qits.orchestrator.peer.PeerAnswer;
import eu.wohlben.qits.orchestrator.peer.PeerCall;
import eu.wohlben.qits.orchestrator.peer.PeerExchange;
import eu.wohlben.qits.orchestrator.run.RunStatus;
import java.util.function.Function;

/**
 * What one step did — everything the executor writes into its row.
 *
 * @param status SUCCEEDED, FAILED or SKIPPED. A body never returns PENDING or RUNNING; those are
 *     the executor's own.
 * @param call the request, or null for a step that never made one
 * @param answer the peer's reply, or null for the same reason
 * @param summary the one human line, or null
 * @param error why it failed or why it was skipped, or null
 * @param skip why it was skipped, when it was — {@link SkipKind}, and null for every other status.
 *     It is what decides whether the steps after it still run, and it is deliberately NOT on the
 *     wire and NOT in a column: it is read once, during the run, by the executor. A reader of a
 *     finished run has the {@code error} sentence, which says the same thing in words.
 */
public record StepResult(
    RunStatus status,
    PeerCall call,
    PeerAnswer answer,
    String summary,
    String error,
    SkipKind skip) {

  /**
   * A step the process chose not to make — a dry run does not sweep.
   *
   * <p><b>This does not cascade.</b> A dependent of a policy-skipped step runs normally, because
   * nothing went wrong: the run did what it was asked. The reason is the wire text, {@code dry
   * run}.
   */
  public static StepResult skipped(String reason) {
    return new StepResult(RunStatus.SKIPPED, null, null, null, reason, SkipKind.POLICY);
  }

  /**
   * A step that refused to make its call because what it would have sent is not evidence — an
   * empty keep-set handed to a deleter, for one.
   *
   * <p><b>FAILED, not skipped</b>: nothing went as asked, and a run that quietly did nothing is
   * the outcome a reader most needs to see. There is no request beside it, because none was made.
   */
  public static StepResult refused(String reason) {
    return new StepResult(RunStatus.FAILED, null, null, null, reason, null);
  }

  /**
   * A step whose dependency FAILED, so it could not honestly run.
   *
   * <p>The executor's own, not a body's. {@code origin} is the step that actually failed rather
   * than the skipped neighbour in between — a reader should not have to walk the graph backwards.
   */
  public static StepResult skippedByFailure(String origin) {
    return new StepResult(
        RunStatus.SKIPPED, null, null, null, "skipped: " + origin + " failed", SkipKind.FAILURE);
  }

  /**
   * The ordinary ending: a call was made, and the peer's status decides the verdict.
   *
   * <p><b>2xx is the only success.</b> A peer that answers 200 with an error object inside is that
   * peer's contract to fix; this service reports what it was told and stores the body whole, so a
   * reader can see the disagreement.
   *
   * @param summary reads the parsed answer and produces the human line. It is only called on a
   *     successful exchange, and it must tolerate a null tree — a bounded body does not parse.
   */
  public static StepResult of(PeerExchange exchange, Function<PeerAnswer, String> summary) {
    PeerAnswer answer = exchange.answer();
    if (answer.error() != null) {
      return new StepResult(RunStatus.FAILED, exchange.call(), answer, null, answer.error(), null);
    }
    if (!answer.ok()) {
      return new StepResult(
          RunStatus.FAILED,
          exchange.call(),
          answer,
          null,
          exchange.call().url() + " answered " + answer.httpStatus(),
          null);
    }
    String line;
    try {
      line = summary.apply(answer);
    } catch (RuntimeException e) {
      // A summary is a convenience. A peer that answered fine and a summariser that could not read
      // its shape is a step that SUCCEEDED with an unreadable caption, never a failed deletion.
      line = "answered " + answer.httpStatus() + "; the summary could not be read: " + e;
    }
    return new StepResult(RunStatus.SUCCEEDED, exchange.call(), answer, line, null, null);
  }

  /**
   * {@link #of}, for a read whose answer is a KEEP-SET: every {@code pins.*} step.
   *
   * <p><b>A 2xx that cannot be read whole is FAILED here, not SUCCEEDED.</b> A pin answer that was
   * cut off, or that is not JSON, says nothing about what must survive. Read as success it reaches
   * the deleters as an absent or empty source — run cd1c3349 summarised a cut-off dependency answer
   * as "0 manifest pins across 0 repositories" (ticket qits-1175). FAILED makes the edges skip every
   * step that deletes.
   */
  public static StepResult ofKeepSet(PeerExchange exchange, Function<PeerAnswer, String> summary) {
    StepResult result = of(exchange, summary);
    if (result.status() != RunStatus.SUCCEEDED) {
      return result;
    }
    PeerAnswer answer = exchange.answer();
    String url = exchange.call().url();
    if (answer.truncated()) {
      return new StepResult(
          RunStatus.FAILED,
          exchange.call(),
          answer,
          null,
          url
              + " answered more than the parse limit; a pin set that cannot be read whole is not"
              + " a pin set",
          null);
    }
    if (answer.json() == null) {
      return new StepResult(
          RunStatus.FAILED,
          exchange.call(),
          answer,
          null,
          url + " answered " + answer.httpStatus() + " with a body that is not JSON; a pin set"
              + " that cannot be parsed is not a pin set",
          null);
    }
    return result;
  }
}
