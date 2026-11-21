package eu.wohlben.qits.orchestrator.contracts;

import static io.restassured.RestAssured.given;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.restassured.response.Response;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * <b>Records qits-orchestrator's provider golden masters</b> — {@code golden-masters/} at the
 * repository root, the source of the published golden-master packages ({@code
 * eu.wohlben.qits:qits-orchestrator-golden-masters}, {@code @qits/orchestrator-golden-masters})
 * consumers write their pacts against: qits-orchestrator-frontend, and the agents and the desk
 * that start gc runs.
 *
 * <p>For each (state, operation) pair in {@link #INTERACTIONS} it runs the state ({@link
 * ProviderStates}), calls the operation as an operator holding {@code qits:admin}, keeps only the
 * list entries the state created, freezes ids and instants ({@link Freezer}) and renders {@code
 * golden-masters/<state-slug>/<operationId>.json}; then it renders {@code golden-masters/index.json}
 * in the format qits-projects-service set (format version 1). A request body is recorded in the
 * index as the operation's {@code body}, as qits-projects-service does for its POSTs.
 *
 * <p><b>What is frozen.</b> A run's id is {@code UUID.randomUUID()} in {@code RunStore.open} and
 * its timings are the clock's, so those become placeholders, numbered sticky to the committed
 * index's params. The step urls are not frozen: they are the {@code %test} target urls the shipped
 * configuration spells ({@code http://dev-qits-<peer>:8080}), fixed in every clone. The run listing
 * is filtered to the state's own run ({@code frozen.listFilteredTo}), whatever else the database
 * holds.
 *
 * <p><b>Operation ids</b> are the ones each operation declares: {@code ProcessController}'s and
 * {@code RunController}'s since ticket qits-2359 (they were recorded under the Java method names
 * before, which is what the OpenAPI derived while neither declared one), {@code
 * ArtifactsCleanupController}'s one since qits-2138 (three until qits-2477). The platform matches an index entry to the
 * OpenAPI by method and path. Renaming one here is a contract change all the same.
 *
 * <p><b>One recording per (state, operationId)</b>: the index keys an operation by that pair, and
 * a second one in a state is a {@code Duplicate interaction} failure. A query that selects a
 * variant of the answer ({@code bodies=false}) is recorded in the operation's {@code query}.
 *
 * <p><b>A refusal is recorded like an answer</b>: an interaction names the status it expects, and
 * {@code a running gc run}'s {@code startProcessRun} records the 409.
 *
 * <p><b>It leaves no run RUNNING.</b> Recording {@code startProcessRun} launches a real (dry) run
 * on the executor against {@link eu.wohlben.qits.orchestrator.peer.FakePeers}; every interaction
 * ends by waiting it out ({@link ProviderStates#awaitNoActiveGcRun}, which also closes the run
 * {@code a running gc run} seeds), so a class after this one does not get a 409 that is not its
 * own.
 *
 * <p>It <b>compares by default</b> and fails with a unified diff per differing file — including a
 * committed {@code .json} no interaction produces any more. {@code -Dgolden.update=true} (or {@code
 * QITS_GOLDEN_UPDATE=true}) rewrites instead, and deletes such stale files; see {@link
 * GoldenFiles}.
 */
@QuarkusTest
@TestProfile(ContractProfile.class)
@TestSecurity(user = "operator", roles = "qits:admin")
class GoldenMasterRecordingTest {

  static final int FORMAT_VERSION = 1;

  /** The application name, as every provider's index names itself. */
  static final String PROVIDER = "qits-orchestrator";

  /**
   * One recorded interaction.
   *
   * @param path the route as a template: a {@code {param}} is expanded from the state's params and
   *     recorded unexpanded
   * @param query the query sent, recorded as the index's {@code query} (as qits-ci-service records
   *     its own); empty for none. Its values are literals — none names a state param here
   * @param body the JSON request body sent, or null for none
   * @param listFilteredTo the array (a {@code $} or {@code $.a.b} path) reduced to the entries the
   *     state created, or null — the index's {@code frozen.listFilteredTo}
   */
  record Interaction(
      String state,
      String operationId,
      String method,
      String path,
      Map<String, String> query,
      String body,
      int status,
      String listFilteredTo) {

    /** An interaction that sends no query. */
    Interaction(
        String state,
        String operationId,
        String method,
        String path,
        String body,
        int status,
        String listFilteredTo) {
      this(state, operationId, method, path, Map.of(), body, status, listFilteredTo);
    }
  }

  static final List<Interaction> INTERACTIONS =
      List.of(
          new Interaction(
              ProviderStates.THE_GC_PROCESS,
              "listProcesses",
              "GET",
              "/orchestrator/api/processes",
              null,
              200,
              null),
          // qits-2359: the run listing before the process has ever run — the whole answer, [].
          new Interaction(
              ProviderStates.THE_GC_PROCESS_BEFORE_ITS_FIRST_RUN,
              "listProcessRuns",
              "GET",
              "/orchestrator/api/processes/{kind}/runs",
              null,
              200,
              null),
          new Interaction(
              ProviderStates.A_FINISHED_GC_DRY_RUN,
              "listProcessRuns",
              "GET",
              "/orchestrator/api/processes/{kind}/runs",
              null,
              200,
              "$"),
          // qits-2359: the run read is recorded body-less. An index entry is one (state,
          // operationId), so this replaces the full read rather than sitting beside it; the step
          // door below is where a consumer is shown a step's bodies.
          new Interaction(
              ProviderStates.A_FINISHED_GC_DRY_RUN,
              "getProcessRun",
              "GET",
              "/orchestrator/api/runs/{runId}",
              Map.of("bodies", "false"),
              null,
              200,
              null),
          new Interaction(
              ProviderStates.A_FINISHED_GC_DRY_RUN,
              "getProcessRunStep",
              "GET",
              "/orchestrator/api/runs/{runId}/steps/{stepId}",
              null,
              200,
              null),
          // qits-2359: a run in flight — its listing row, its body-less read, and the 409 a start
          // is refused with while it stands.
          new Interaction(
              ProviderStates.A_RUNNING_GC_RUN,
              "listProcessRuns",
              "GET",
              "/orchestrator/api/processes/{kind}/runs",
              null,
              200,
              "$"),
          new Interaction(
              ProviderStates.A_RUNNING_GC_RUN,
              "getProcessRun",
              "GET",
              "/orchestrator/api/runs/{runId}",
              Map.of("bodies", "false"),
              null,
              200,
              null),
          new Interaction(
              ProviderStates.A_RUNNING_GC_RUN,
              "startProcessRun",
              "POST",
              "/orchestrator/api/processes/{kind}/runs",
              "{\"dryRun\": true}",
              409,
              null),
          new Interaction(
              ProviderStates.NO_ACTIVE_GC_RUN,
              "startProcessRun",
              "POST",
              "/orchestrator/api/processes/{kind}/runs",
              "{\"dryRun\": true}",
              202,
              null),
          // The artifacts cleanup listing (ticket qits-2138): qits-artifacts' answer, handed on.
          new Interaction(
              ProviderStates.A_REPOSITORY_THE_CLEANUP_CAN_PLAN,
              "listArtifactsCleanupRepositories",
              "GET",
              "/orchestrator/api/artifacts/repositories",
              null,
              200,
              null));

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern TEMPLATE_PARAM = Pattern.compile("\\{([^}]+)}");

  @Inject ProviderStates states;

  /**
   * The committed index's frozen params, slug → param → frozen value: what pins a param's frozen id
   * number ({@link Freezer#seed(Map, Map)}), read once before recording.
   */
  private Map<String, Map<String, String>> committedParams = Map.of();

  @Test
  void goldenMastersMatchTheProvider() throws IOException {
    Path dir = GoldenFiles.repositoryRoot().resolve("golden-masters");
    boolean update = GoldenFiles.updating();
    List<String> failures = new ArrayList<>();
    Set<String> written = new TreeSet<>();

    // slug -> recorded state, sorted by slug; operations sorted by operationId below
    Map<String, ObjectNode> indexStates = new TreeMap<>();
    Map<String, Map<String, ObjectNode>> indexOperations = new TreeMap<>();

    committedParams = committedParams(dir.resolve("index.json"));
    for (Interaction interaction : INTERACTIONS) {
      Recorded recorded = record(interaction);
      String slug = ProviderStates.slug(interaction.state());
      String file = slug + "/" + interaction.operationId() + ".json";

      ObjectNode state = indexStates.get(slug);
      if (state == null) {
        state = JsonNodeFactory.instance.objectNode();
        state.put("name", interaction.state());
        state.put("slug", slug);
        state.set("params", recorded.params());
        state.set("dependsOn", JsonNodeFactory.instance.arrayNode());
        indexStates.put(slug, state);
      } else if (!state.get("params").equals(recorded.params())) {
        failures.add("State '" + interaction.state() + "' froze to different params per operation");
      }

      ObjectNode operation = JsonNodeFactory.instance.objectNode();
      operation.put("operationId", interaction.operationId());
      operation.put("method", interaction.method());
      operation.put("path", interaction.path());
      if (!interaction.query().isEmpty()) {
        ObjectNode query = operation.putObject("query");
        new TreeMap<>(interaction.query()).forEach(query::put);
      }
      if (interaction.body() != null) {
        operation.set("body", JSON.readTree(interaction.body()));
      }
      operation.put("status", interaction.status());
      operation.put("file", file);
      ObjectNode frozen = operation.putObject("frozen");
      frozen.set("ids", strings(recorded.freezer().idPaths()));
      frozen.set("instants", strings(recorded.freezer().instantPaths()));
      frozen.set("strings", strings(recorded.freezer().stringPaths()));
      if (interaction.listFilteredTo() == null) {
        frozen.putNull("listFilteredTo");
      } else {
        frozen.put("listFilteredTo", interaction.listFilteredTo());
      }
      if (indexOperations
              .computeIfAbsent(slug, k -> new TreeMap<>())
              .put(interaction.operationId(), operation)
          != null) {
        failures.add("Duplicate interaction " + file);
      }

      written.add(file);
      check(dir.resolve(file), GoldenJson.render(recorded.body()), update, failures);
    }

    ObjectNode index = JsonNodeFactory.instance.objectNode();
    index.put("formatVersion", FORMAT_VERSION);
    index.put("provider", PROVIDER);
    ArrayNode stateArray = index.putArray("states");
    indexStates.forEach(
        (slug, state) -> {
          ArrayNode operations = state.putArray("operations");
          indexOperations.get(slug).values().forEach(operations::add);
          stateArray.add(state);
        });
    written.add("index.json");
    check(dir.resolve("index.json"), GoldenJson.render(index), update, failures);

    for (String stale : committedJson(dir)) {
      if (written.contains(stale)) {
        continue;
      }
      if (update) {
        Files.delete(dir.resolve(stale));
      } else {
        failures.add(
            dir.resolve(stale)
                + " is committed but no interaction records it any more — rerun with"
                + " -Dgolden.update=true to delete it.");
      }
    }

    if (!failures.isEmpty()) {
      throw new AssertionError(String.join("\n\n", failures));
    }
  }

  private static void check(Path golden, String actual, boolean update, List<String> failures) {
    String failure = GoldenFiles.check(golden, actual, update, UnaryOperator.identity());
    if (failure != null) {
      failures.add(failure);
    }
  }

  /** One interaction's frozen answer, its frozen params and what was frozen where. */
  record Recorded(JsonNode body, ObjectNode params, Freezer freezer) {}

  private Recorded record(Interaction interaction) throws IOException {
    Map<String, String> params = states.params(interaction.state());

    Response response;
    try {
      var request = given().header("Accept", "application/json").queryParams(interaction.query());
      if (interaction.body() != null) {
        request = request.contentType("application/json").body(interaction.body());
      } else {
        // As a browser sends a body-less call: RestAssured would otherwise add a form content type.
        request = request.noContentType();
      }
      response = request.when().request(interaction.method(), expand(interaction.path(), params));
    } finally {
      // A start leaves its run on the executor; nothing after this may find it RUNNING.
      states.awaitNoActiveGcRun();
    }
    String raw = response.asString();
    if (response.statusCode() != interaction.status()) {
      throw new AssertionError(
          interaction.method()
              + " "
              + interaction.path()
              + " in state '"
              + interaction.state()
              + "' answered "
              + response.statusCode()
              + ", expected "
              + interaction.status()
              + ": "
              + raw);
    }
    JsonNode body = recordable(JSON.readTree(raw), interaction, params.values());

    Freezer freezer =
        new Freezer()
            .seed(
                params,
                committedParams.getOrDefault(ProviderStates.slug(interaction.state()), Map.of()));
    ObjectNode frozenParams = JsonNodeFactory.instance.objectNode();
    params.forEach((k, v) -> frozenParams.put(k, freezer.freezeParam(v)));
    return new Recorded(freezer.freeze(body), frozenParams, freezer);
  }

  /**
   * The answer reduced to what the state controls: the {@code listFilteredTo} array keeps only the
   * entries mentioning a value the state created (its param values).
   */
  static JsonNode recordable(JsonNode body, Interaction interaction, Collection<String> created) {
    JsonNode out = body.deepCopy();
    if (interaction.listFilteredTo() != null) {
      ArrayNode list = array(out, interaction.listFilteredTo());
      ArrayNode kept = JsonNodeFactory.instance.arrayNode();
      for (JsonNode entry : list) {
        String text = entry.toString();
        // The kind is a param too, and every gc run names it: only the state's own ids select.
        if (created.stream()
            .filter(value -> Freezer.UUID.matcher(value).matches())
            .anyMatch(text::contains)) {
          kept.add(entry);
        }
      }
      list.removeAll();
      list.addAll(kept);
    }
    return out;
  }

  /** The array at {@code $} or a {@code $.a.b} path — the only JSONPath shapes the table uses. */
  private static ArrayNode array(JsonNode root, String path) {
    JsonNode node = root;
    if (!path.equals("$")) {
      if (!path.startsWith("$.")) {
        throw new IllegalArgumentException("Only $ and $.a.b paths are supported: " + path);
      }
      for (String segment : path.substring(2).split("\\.")) {
        node = node.path(segment);
      }
    }
    if (!node.isArray()) {
      throw new IllegalStateException(path + " is not an array in " + root);
    }
    return (ArrayNode) node;
  }

  private static String expand(String template, Map<String, String> params) {
    Matcher m = TEMPLATE_PARAM.matcher(template);
    StringBuilder out = new StringBuilder();
    while (m.find()) {
      String value = params.get(m.group(1));
      if (value == null) {
        throw new IllegalStateException(
            "Path " + template + " names {" + m.group(1) + "}, which the state does not return");
      }
      m.appendReplacement(out, Matcher.quoteReplacement(value));
    }
    m.appendTail(out);
    return out.toString();
  }

  private static ArrayNode strings(List<String> values) {
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    values.forEach(out::add);
    return out;
  }

  /**
   * slug → frozen params of every state in the committed index ({@code states[].slug}, {@code
   * states[].params}); empty when there is no index yet.
   */
  static Map<String, Map<String, String>> committedParams(Path index) throws IOException {
    if (!Files.isRegularFile(index)) {
      return Map.of();
    }
    Map<String, Map<String, String>> out = new TreeMap<>();
    for (JsonNode state : JSON.readTree(index.toFile()).path("states")) {
      Map<String, String> params = new TreeMap<>();
      state
          .path("params")
          .fields()
          .forEachRemaining(p -> params.put(p.getKey(), p.getValue().asText()));
      out.put(state.path("slug").asText(), params);
    }
    return out;
  }

  /** Every committed {@code .json} under the directory, relative and {@code /}-separated. */
  private static List<String> committedJson(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) {
      return List.of();
    }
    try (Stream<Path> files = Files.walk(dir)) {
      return files
          .filter(Files::isRegularFile)
          .filter(p -> p.getFileName().toString().endsWith(".json"))
          .map(p -> dir.relativize(p).toString().replace('\\', '/'))
          .sorted()
          .toList();
    }
  }
}
