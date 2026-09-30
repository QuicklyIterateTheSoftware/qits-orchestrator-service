package eu.wohlben.qits.orchestrator.process.gc;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Locale;

/**
 * The one human line each gc step leaves behind, read out of the peer's own answer.
 *
 * <p><b>Nothing here is computed twice.</b> Every figure is taken from the document that reports
 * it — a summary that re-derived "what would die" would be a second policy, and two policies in one
 * report is the mistake the whole design refuses. qits-artifacts' own {@code GcSummary} says the
 * same thing about the same numbers.
 *
 * <p><b>Every reader tolerates a missing field.</b> A peer that answered 200 with a shape this does
 * not recognise is a SUCCEEDED step with a thin caption, never a failed deletion: the response body
 * is stored whole beside it, so nothing is lost by a summary that cannot read it.
 */
final class GcSummaries {

  private GcSummaries() {}

  /** {@code images 43.5 GB (19.3 GB reclaimable), build cache 35.1 GB} */
  static String usage(JsonNode body) {
    if (body == null) {
      return "no usage figures in the answer";
    }
    JsonNode images = body.path("images");
    JsonNode cache = body.path("buildCache");
    return "images "
        + bytes(images.path("sizeBytes").asLong())
        + " ("
        + bytes(images.path("reclaimableBytes").asLong())
        + " reclaimable), build cache "
        + bytes(cache.path("sizeBytes").asLong());
  }

  /**
   * {@code store 51.2 GB (oci 50.7 GB, docs 164.2 MB, sboms 112.6 MB)} — the registry's own bytes.
   *
   * <p><b>The registry plane is invisible to the docker read beside it.</b> {@link #usage} is the
   * host's images, containers, volumes and buildkit cache; a registry blob is a row and a file that
   * belong to qits-artifacts, so a run measured only by {@code docker system df} reports a platform
   * that is not growing while the store behind it does. The 2026-09-04 storage incident was 50 GB
   * nobody's receipt showed.
   */
  static String storeUsage(JsonNode body) {
    if (body == null) {
      return "no store figures in the answer";
    }
    return "store "
        + bytes(body.path("diskTotalBytes").asLong())
        + " (oci "
        + bytes(body.path("ociUnionBytes").asLong())
        + ", docs "
        + bytes(body.path("docsBytes").asLong())
        + ", sboms "
        + bytes(body.path("sbomBytes").asLong())
        + ")";
  }

  /** {@code 51 repositories in the catalogue} — the iteration set of the branch sweep. */
  static String repositoryCatalogue(JsonNode body) {
    int repositories = body == null ? 0 : body.path("repositories").size();
    return repositories
        + (repositories == 1 ? " repository" : " repositories")
        + " in the catalogue";
  }

  /**
   * {@code removed 3 of 214 branches across 51 repositories (dry run), 1 error} — the sweep's own
   * count of what it deleted, or on a dry run would have.
   */
  static String branchesSweep(JsonNode body) {
    if (body == null) {
      return "no sweep report in the answer";
    }
    int removed = body.path("removed").size();
    int examined = body.path("branchesExamined").asInt();
    int repositories = body.path("repositoriesExamined").asInt();
    int errors = body.path("errors").size();
    return "removed "
        + removed
        + " of "
        + examined
        + " branches across "
        + repositories
        + " repositories"
        + (body.path("dryRun").asBoolean() ? " (dry run)" : "")
        + (errors == 0 ? "" : ", " + errors + (errors == 1 ? " error" : " errors"));
  }

  /** {@code 14 applications pinned} — the deployments pin answer. */
  static String deploymentPins(JsonNode body) {
    int applications = body == null ? 0 : body.path("pins").size();
    return applications + (applications == 1 ? " application pinned" : " applications pinned");
  }

  /** {@code qits-ci-daemon 2026.815.120000 (previous 2026.814.101010)} — the ci pin answer. */
  static String ciPin(JsonNode body) {
    if (body == null) {
      return "no daemon pin in the answer";
    }
    String name = text(body, "daemonName", "the ci daemon");
    String version = text(body, "daemonVersion", "");
    String previous = text(body, "previousDaemonVersion", "");
    if (version.isBlank()) {
      return name + " pins nothing";
    }
    return name + " " + version + (previous.isBlank() ? "" : " (previous " + previous + ")");
  }

  /**
   * {@code 412 manifest pins across 47 repositories} — the maintenance dependency-pin answer: what
   * repositories' mains still reference, which no deployment and no release date can say.
   */
  static String dependencyPins(JsonNode body) {
    int pins = body == null ? 0 : body.path("pins").size();
    int repositories = body == null ? 0 : body.path("repositories").size();
    return pins
        + (pins == 1 ? " manifest pin across " : " manifest pins across ")
        + repositories
        + (repositories == 1 ? " repository" : " repositories");
  }

  /**
   * {@code 4 configured container images} — the configuration pin answer: the versions the NEXT
   * deploy of a launching service would be given.
   */
  static String imagePins(JsonNode body) {
    int images = body == null ? 0 : body.path("pins").size();
    return images
        + (images == 1 ? " configured container image" : " configured container images");
  }

  /**
   * {@code 2 launch images — what a workspace/editor start would pull today} — qits-workspaces'
   * own answer, read out of the config it is actually running with.
   *
   * <p><b>"Today" is the whole distinction from {@link #imagePins}.</b> The configured version is
   * what the next deploy will hand a service; the effective one is what the service running now
   * would pull, and it lags until that deploy happens. Only the consumer can say which it is.
   */
  static String workspaceLaunchPins(JsonNode body) {
    return launchPins(body, "a workspace/editor start");
  }

  /**
   * {@code 2 launch images — what an agent/refinement start would pull today} — qits-projects'
   * own answer, on the same terms.
   */
  static String projectLaunchPins(JsonNode body) {
    return launchPins(body, "an agent/refinement start");
  }

  private static String launchPins(JsonNode body, String start) {
    int images = body == null ? 0 : body.path("pins").size();
    return images
        + (images == 1 ? " launch image — what " : " launch images — what ")
        + start
        + " would pull today";
  }

  /** {@code 128 identities, 19.3 GB reclaimable, executable=true} — the artifacts plan answer. */
  static String artifactsPlan(JsonNode body) {
    JsonNode summary = body == null ? null : body.get("summary");
    if (summary == null) {
      return "no plan summary in the answer";
    }
    return summary.path("identitiesCondemned").asInt()
        + " identities, "
        + text(summary, "reclaimable", bytes(summary.path("reclaimableBytes").asLong()))
        + " reclaimable, executable="
        + summary.path("executable").asBoolean();
  }

  /** {@code 91 blobs unlinked, 17.8 GB reclaimed} — the artifacts sweep answer. */
  static String artifactsSweep(JsonNode body) {
    JsonNode sweep = body == null ? null : body.get("sweep");
    if (sweep == null) {
      String aborted = body == null ? null : text(body, "aborted", "");
      return aborted == null || aborted.isBlank()
          ? "no sweep outcome in the answer"
          : "aborted: " + aborted;
    }
    return sweep.path("blobsUnlinked").asInt()
        + " blobs unlinked, "
        + bytes(sweep.path("bytesReclaimed").asLong())
        + " reclaimed";
  }

  /** {@code 12 images removed, 9.4 GB reclaimed, 61 kept} */
  static String images(JsonNode body) {
    if (body == null) {
      return "no image figures in the answer";
    }
    return body.path("removed").size()
        + " images removed, "
        + bytes(body.path("bytesReclaimed").asLong())
        + " reclaimed, "
        + body.path("kept").size()
        + " kept";
  }

  /** {@code 4 volumes removed, 9 kept} — no bytes, because the peer reports none. */
  static String volumes(JsonNode body) {
    if (body == null) {
      return "no volume figures in the answer";
    }
    return body.path("removed").size()
        + " volumes removed, "
        + body.path("kept").size()
        + " kept";
  }

  /** {@code host 12.6 GB reclaimed, 2 builders 3.1 GB reclaimed} */
  static String buildCache(JsonNode body) {
    if (body == null) {
      return "no build cache figures in the answer";
    }
    long host = body.path("host").path("reclaimedBytes").asLong();
    JsonNode builders = body.path("builders");
    long fromBuilders = 0;
    for (JsonNode builder : builders) {
      fromBuilders += builder.path("reclaimedBytes").asLong();
    }
    return "host "
        + bytes(host)
        + " reclaimed, "
        + builders.size()
        + (builders.size() == 1 ? " builder " : " builders ")
        + bytes(fromBuilders)
        + " reclaimed";
  }

  /**
   * {@code removed 8 of 142 entries (dry run); kept: unpinned 12, undeclaredPinnedVersion 3, pinned
   * 10, inFlight 2, neverDeclared 85, staged 4; 1 error} — qits-configuration's own retired-entry
   * sweep.
   *
   * <p><b>Retired, not merely unpinned.</b> An entry here is a key some declaration of the
   * application used to state and that no surviving declaration states any longer — neither a
   * serving nor a rollback version, the {@code pins.deployments} answer this step hands over. The
   * rule for what counts as retired lives with qits-configuration; this reads only what it reports.
   */
  static String configurationEntries(JsonNode body) {
    if (body == null) {
      return "no entry-gc report in the answer";
    }
    int removed = body.path("removed").size();
    int examined = body.path("examined").asInt();
    int errors = body.path("errors").size();
    JsonNode kept = body.path("kept");
    return "removed "
        + removed
        + " of "
        + examined
        + " entries"
        + (body.path("dryRun").asBoolean() ? " (dry run)" : "")
        + "; kept: unpinned "
        + kept.path("unpinned").asInt()
        + ", undeclaredPinnedVersion "
        + kept.path("undeclaredPinnedVersion").asInt()
        + ", pinned "
        + kept.path("pinned").asInt()
        + ", inFlight "
        + kept.path("inFlight").asInt()
        + ", neverDeclared "
        + kept.path("neverDeclared").asInt()
        + ", staged "
        + kept.path("staged").asInt()
        + "; "
        + errors
        + (errors == 1 ? " error" : " errors");
  }

  private static String text(JsonNode node, String field, String fallback) {
    JsonNode value = node == null ? null : node.get(field);
    return value == null || value.isNull() ? fallback : value.asText();
  }

  /**
   * Bytes as a person reads them. Powers of 1000 with the SI names, because that is what {@code
   * docker system df} prints and a report that disagreed with the command it summarises would be
   * read as wrong.
   */
  static String bytes(long value) {
    if (value < 1000) {
      return value + " B";
    }
    String[] units = {"kB", "MB", "GB", "TB", "PB"};
    double scaled = value;
    int unit = -1;
    while (scaled >= 1000 && unit < units.length - 1) {
      scaled /= 1000;
      unit++;
    }
    return String.format(Locale.ROOT, "%.1f %s", scaled, units[unit]);
  }
}
