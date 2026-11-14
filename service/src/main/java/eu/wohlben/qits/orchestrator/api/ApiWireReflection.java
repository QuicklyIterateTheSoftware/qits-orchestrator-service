package eu.wohlben.qits.orchestrator.api;

import eu.wohlben.qits.orchestrator.dto.ProcessDto;
import eu.wohlben.qits.orchestrator.dto.RequestDto;
import eu.wohlben.qits.orchestrator.dto.RunDetailDto;
import eu.wohlben.qits.orchestrator.dto.RunStepDto;
import eu.wohlben.qits.orchestrator.dto.RunSummaryDto;
import eu.wohlben.qits.orchestrator.dto.StepDto;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Native-image reflection registration for every type Jackson touches on this API.
 *
 * <p>{@code ProcessController.start} returns {@code Response.entity(...)}, which hides the entity
 * type from the build-time analysis — so in the native binary serialization fails at runtime with a
 * 500 while every JVM test stays green. Measured on a sibling, not theoretical:
 * qits-serviceregistry's first live {@code PUT /services/{name}} answered 500 on exactly this.
 *
 * <p>Some of these types happen to be reachable today through a declared return type; they are all
 * listed anyway, because which ones the analysis finds is an implementation detail no test guards.
 *
 * <p>{@link ArtifactsCleanupController} adds nothing here: it answers qits-artifacts' text as a
 * {@code String} and its own 502 as a {@code Map}, neither of which needs registering (ticket
 * qits-2138).
 *
 * <p><b>A new response type joins this list in the commit that adds it.</b>
 */
@RegisterForReflection(
    targets = {
      ProcessController.StartRunRequest.class,
      ProcessController.StartRunRequest.Response.class,
      ProcessDto.class,
      StepDto.class,
      RunSummaryDto.class,
      RunDetailDto.class,
      RunStepDto.class,
      RequestDto.class
    })
final class ApiWireReflection {

  private ApiWireReflection() {}
}
