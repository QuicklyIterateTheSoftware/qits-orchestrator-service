package eu.wohlben.qits.orchestrator.process.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * The five readers the pin sources brought with them — the registry plane's, and the two effective
 * launch reads — over the answers they are written for and over answers they are not.
 *
 * <p><b>A missing field answers a sentence, never a throw.</b> That is the rule the whole class
 * keeps — a peer that answered 200 with a shape this does not recognise is a SUCCEEDED step with a
 * thin caption, because the body is stored whole beside it and nothing is lost by a summary that
 * could not read it. It is asserted here rather than only through a run: a summariser that threw
 * would be caught by {@code StepResult.of} and turned into a caption nobody would read twice.
 *
 * <p>Not a {@code @QuarkusTest}: these are functions of a tree.
 */
class GcSummariesTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static JsonNode json(String text) {
    try {
      return JSON.readTree(text);
    } catch (Exception e) {
      throw new IllegalStateException("not JSON: " + text, e);
    }
  }

  @Test
  void theStoreSummaryIsTheRegistrysOwnFourFigures() {
    assertEquals(
        "store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)",
        GcSummaries.storeUsage(
            json(
                """
                {"diskTotalBytes":51200000000,"ociUnionBytes":50700000000,
                 "docsBytes":164200000,"sbomBytes":112600000}
                """)));
  }

  @Test
  void aStoreAnswerWithNothingInItReadsAsZeroesRatherThanFailing() {
    assertEquals("no store figures in the answer", GcSummaries.storeUsage(null));
    assertEquals(
        "store 0 B (oci 0 B, docs 0 B, sboms 0 B)", GcSummaries.storeUsage(json("{\"nope\":1}")));
  }

  @Test
  void theDependencyPinsCountBothTheReferencesAndWhoMakesThem() {
    assertEquals(
        "3 manifest pins across 2 repositories",
        GcSummaries.dependencyPins(
            json(
                """
                {"repositories":[{"name":"qits-githost-service"},{"name":"qits-workspace-daemon"}],
                 "pins":[{"ecosystem":"maven","name":"eu.wohlben.qits:qits-blobstore"},
                         {"ecosystem":"npm","name":"@qits/ui-components"},
                         {"ecosystem":"docker","name":"qits/workspace-base"}]}
                """)));
    // One of each, because a plural that reads "1 manifest pins" is the kind of line an operator
    // stops trusting.
    assertEquals(
        "1 manifest pin across 1 repository",
        GcSummaries.dependencyPins(
            json("{\"repositories\":[{\"name\":\"qits-ci-service\"}],\"pins\":[{\"name\":\"a\"}]}")));
  }

  @Test
  void aDependencyAnswerThisReaderDoesNotRecogniseIsStillASentence() {
    assertEquals("0 manifest pins across 0 repositories", GcSummaries.dependencyPins(null));
    assertEquals(
        "0 manifest pins across 0 repositories",
        GcSummaries.dependencyPins(json("{\"message\":\"never scanned\"}")));
  }

  @Test
  void theImagePinsAreCountedAndAnEmptySetIsAValidAnswer() {
    assertEquals(
        "2 configured container images",
        GcSummaries.imagePins(
            json(
                """
                {"pins":[{"image":"qits/project-agent","version":"2026.904.160152"},
                         {"image":"qits/workspace","version":"2026.904.160522"}]}
                """)));
    assertEquals("1 configured container image", GcSummaries.imagePins(json("{\"pins\":[{}]}")));
    // Nothing released is nothing to keep, and it is an answer rather than a fault.
    assertEquals("0 configured container images", GcSummaries.imagePins(json("{\"pins\":[]}")));
    assertEquals("0 configured container images", GcSummaries.imagePins(null));
  }

  @Test
  void theLaunchPinsSayWhoseStartTheyAreAboutAndThatItIsTodays() {
    // "today" rather than "configured", because that IS the distinction the two sources exist for:
    // qits-configuration says what the next deploy will hand a service, and these say what the
    // service running now would actually pull. A caption that read the same as imagePins would hide
    // the only reason there are six pin sources rather than four.
    assertEquals(
        "2 launch images — what a workspace/editor start would pull today",
        GcSummaries.workspaceLaunchPins(
            json(
                """
                {"pins":[{"image":"qits/workspace","version":"2026.903.120000",
                          "launches":"workspace"},
                         {"image":"qits/workspace-editor","version":"2026.904.100239",
                          "launches":"editor"}]}
                """)));
    assertEquals(
        "2 launch images — what an agent/refinement start would pull today",
        GcSummaries.projectLaunchPins(
            json(
                """
                {"pins":[{"image":"qits/project-agent","version":"2026.903.090000",
                          "launches":"agent"},
                         {"image":"qits/workspace","version":"2026.903.120000",
                          "launches":"refinement"}]}
                """)));
  }

  @Test
  void theConfigurationEntrySweepReportsWhatWasRemovedAndWhatWasKept() {
    assertEquals(
        "removed 8 of 142 entries (dry run); kept: unpinned 12, undeclaredPinnedVersion 3,"
            + " pinned 10, inFlight 2, neverDeclared 85, staged 4; 1 error",
        GcSummaries.configurationEntries(
            json(
                """
                {"dryRun":true,"examined":142,
                 "removed":[{"application":"qits-ci","env":"dev","key":"env.OLD_FLAG",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-ci","env":"dev","key":"env.OLD_FLAG_2",
                             "reason":"retired","lastDeclaredBy":"2026.814.101010"},
                            {"application":"qits-gateway","env":"dev","key":"env.A",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-gateway","env":"dev","key":"env.B",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-gateway","env":"dev","key":"env.C",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-gateway","env":"dev","key":"env.D",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-gateway","env":"dev","key":"env.E",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"},
                            {"application":"qits-gateway","env":"dev","key":"env.F",
                             "reason":"retired","lastDeclaredBy":"2026.815.120000"}],
                 "kept":{"unpinned":12,"undeclaredPinnedVersion":3,"pinned":10,"inFlight":2,
                         "neverDeclared":85,"staged":4},
                 "errors":[{"application":"qits-workspaces","message":"timeout"}]}
                """)));
  }

  @Test
  void theConfigurationEntrySweepOnANonDryRunOmitsTheDryRunSuffix() {
    assertEquals(
        "removed 0 of 0 entries; kept: unpinned 0, undeclaredPinnedVersion 0, pinned 0,"
            + " inFlight 0, neverDeclared 0, staged 0; 0 errors",
        GcSummaries.configurationEntries(
            json(
                """
                {"dryRun":false,"examined":0,"removed":[],
                 "kept":{"unpinned":0,"undeclaredPinnedVersion":0,"pinned":0,"inFlight":0,
                         "neverDeclared":0,"staged":0},
                 "errors":[]}
                """)));
  }

  @Test
  void aConfigurationEntryAnswerThisReaderDoesNotRecogniseIsStillASentence() {
    assertEquals("no entry-gc report in the answer", GcSummaries.configurationEntries(null));
    assertEquals(
        "removed 0 of 0 entries; kept: unpinned 0, undeclaredPinnedVersion 0, pinned 0,"
            + " inFlight 0, neverDeclared 0, staged 0; 0 errors",
        GcSummaries.configurationEntries(json("{\"message\":\"never scanned\"}")));
  }

  @Test
  void theTagSweepReportsWhatWasDecommissionedByHostAndTwinAndWhatWasKept() {
    assertEquals(
        "3 tags decommissioned across 2 repositories (2 host, 2 twin); kept: newest 40,"
            + " pinnedVersion 6, gitlink 3, inFlight 1, young 2; 1 error",
        GcSummaries.tagsSweep(
            json(
                """
                {"dryRun":false,"repositories":2,"examined":52,
                 "deleted":[{"repository":"qits-ci","tag":"2026.814.090000","host":true,"twin":true},
                            {"repository":"qits-ci","tag":"2026.813.070000","host":true,"twin":false},
                            {"repository":"qits-docs","tag":"2026.812.050000","host":false,
                             "twin":true}],
                 "kept":{"newest":40,"pinnedVersion":6,"gitlink":3,"inFlight":1,"young":2},
                 "errors":["qits-gateway: push failed"]}
                """)));
  }

  @Test
  void theTagSweepOnADryRunSaysWouldDecommissionInsteadOfDecommissioned() {
    assertEquals(
        "1 tags would be decommissioned across 1 repositories (1 host, 0 twin); kept: newest 0,"
            + " pinnedVersion 0, gitlink 0, inFlight 0, young 0; 0 errors",
        GcSummaries.tagsSweep(
            json(
                """
                {"dryRun":true,"repositories":1,"examined":1,
                 "deleted":[{"repository":"qits-ci","tag":"2026.814.090000","host":true,
                             "twin":false}],
                 "kept":{"newest":0,"pinnedVersion":0,"gitlink":0,"inFlight":0,"young":0},
                 "errors":[]}
                """)));
  }

  @Test
  void aTagSweepAnswerThisReaderDoesNotRecogniseIsStillASentence() {
    assertEquals("no tag-sweep report in the answer", GcSummaries.tagsSweep(null));
    assertEquals(
        "0 tags decommissioned across 0 repositories (0 host, 0 twin); kept: newest 0,"
            + " pinnedVersion 0, gitlink 0, inFlight 0, young 0; 0 errors",
        GcSummaries.tagsSweep(json("{\"message\":\"never scanned\"}")));
  }

  @Test
  void aLaunchPinAnswerCanBeSingularEmptyOrUnreadableAndIsStillASentence() {
    assertEquals(
        "1 launch image — what a workspace/editor start would pull today",
        GcSummaries.workspaceLaunchPins(json("{\"pins\":[{\"image\":\"qits/workspace\"}]}")));
    // A service with no version set launches nothing pinnable, and that is an answer rather than a
    // fault — the contract says a blank version omits the row.
    assertEquals(
        "0 launch images — what an agent/refinement start would pull today",
        GcSummaries.projectLaunchPins(json("{\"pins\":[]}")));
    assertEquals(
        "0 launch images — what a workspace/editor start would pull today",
        GcSummaries.workspaceLaunchPins(null));
    assertEquals(
        "0 launch images — what an agent/refinement start would pull today",
        GcSummaries.projectLaunchPins(json("{\"message\":\"forbidden\"}")));
  }
}
