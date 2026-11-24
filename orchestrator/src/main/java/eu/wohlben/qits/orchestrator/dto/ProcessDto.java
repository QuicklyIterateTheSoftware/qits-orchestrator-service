package eu.wohlben.qits.orchestrator.dto;

import java.util.List;

/**
 * One technical process, as {@code GET /processes} serves it: what it is and what it would do.
 *
 * @param kind the id in every url, e.g. {@code gc}
 * @param name the human name
 * @param description one paragraph a person reads before pressing the button
 * @param dryRunnable whether a run may be a dry run; false for a process whose peer door has no
 *     such flag (ticket qits-2490), and a start asking for one is then a 400
 * @param steps the steps in declaration order
 */
public record ProcessDto(
    String kind, String name, String description, boolean dryRunnable, List<StepDto> steps) {}
