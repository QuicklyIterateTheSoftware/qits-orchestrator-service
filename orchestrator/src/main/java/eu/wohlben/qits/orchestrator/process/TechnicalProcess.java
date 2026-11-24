package eu.wohlben.qits.orchestrator.process;

import java.util.List;

/**
 * A technical process: a named, ordered list of steps that only send requests.
 *
 * <p><b>What a process may do is the whole rule of this repository.</b> It calls other services and
 * records what they said. It holds no socket, opens no store of its own beyond the run log, and
 * makes no decision an owner has not published as an API. A process that wanted to delete something
 * itself would be a second opinion about a rule that lives with the store's owner.
 *
 * <p>Implementations are CDI beans discovered by {@code ProcessRegistry}. Adding one is a class and
 * nothing else.
 */
public interface TechnicalProcess {

  /** The stable kind, used in every route: {@code gc}. */
  String kind();

  /** The human name, e.g. "Garbage collection". */
  String name();

  /** One paragraph a person reads before pressing the button. */
  String description();

  /**
   * The steps, in DECLARATION ORDER — which is the order they run in.
   *
   * <p>Order and edges are separate things. The order is this list; the edges are each step's
   * {@code dependsOn}, and they decide only what a failure skips. A step whose dependencies all
   * succeeded runs when the list reaches it, so a process is read top to bottom.
   */
  List<StepDefinition> steps();

  /**
   * Whether a run of this process may be a DRY run — one that calls every peer but deletes or
   * changes nothing. True for gc, whose every deleter takes the peer's own {@code dryRun} flag.
   *
   * <p><b>False where no peer door has such a flag</b> (ticket qits-2490): a manifest scan and the
   * SBOM check each call one qits-maintenance door that simply does its work. A "dry" run of them
   * would be a real run wearing the wrong label, so {@code POST /processes/{kind}/runs} refuses
   * {@code {"dryRun":true}} for such a kind with a 400 rather than starting one, and {@code GET
   * /processes} says so, so a client hides the button instead of offering a refusal.
   */
  default boolean dryRunnable() {
    return true;
  }

  /**
   * Whether a caller holding {@code qits:agent} alone — a commissioned agent — may start a run of
   * this process. Every other role the routes admit may start any process.
   *
   * <p>False by default: an owner decided an agent may start a gc run (dry or real), and that
   * decision was made for gc. A process that files tickets or loads a peer for an hour is not
   * covered by it until an owner says so here.
   */
  default boolean agentStartable() {
    return false;
  }
}
