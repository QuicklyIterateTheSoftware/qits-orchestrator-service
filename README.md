# qits-platform-orchestrator

**Technical processes**: multi-step jobs that only send requests to other services and record what
happened. One process exists, `gc` — the platform's unified deletion run.

It deletes nothing itself and holds no docker socket. Registry blobs, tags and manifests are
qits-artifacts' own GC engine; host images, orphan volumes and buildkit cache are qits-containers',
which is the component that holds the socket. This service reads the platform's pin set once, hands
it to every deleter, calls each owner's API in dependency order, and keeps the account.

Naming note: qits-containers' own docs call it "the container orchestrator". That word has two
meanings on the platform now; the containers repo keeps its prose, this service is "the
orchestrator" in navigation and in conversation.

The contract — routes, step ids, edges, request and response shapes — is pinned by
`qits-orchestrator-plan.md` in the qits-qits wrapper. Four repositories build against it.

## The gc process

Steps run in declaration order. An edge is a requirement, not an ordering hint: a **failed** step
marks every transitive dependent SKIPPED, while independent steps still run. A step the process
**chose** not to make — `artifacts.sweep` on a dry run — is a satisfied dependency, not a failure,
so what comes after it still runs.

| id | target | call | depends on |
|---|---|---|---|
| `usage.before` | containers | `GET /containers/api/gc/usage` | — |
| `artifacts.usage.before` | artifacts | `GET /artifacts/api/store/summary` | — |
| `pins.deployments` | deployments | `GET /deployments/api/pins` | — |
| `pins.ci` | ci | `GET /ci/api/daemon` | — |
| `pins.dependencies` | maintenance | `GET /maintenance/api/pins` | — |
| `pins.images` | configuration | `GET /configuration/api/pins` | — |
| `pins.workspaces` | workspaces | `GET /workspaces/api/pins` | — |
| `pins.projects` | projects | `GET /projects/api/pins` | — |
| `artifacts.plan` | artifacts | `POST /artifacts/api/gc/plan {pins}` | pins.deployments, pins.ci, pins.dependencies, pins.images, pins.workspaces, pins.projects |
| `artifacts.sweep` | artifacts | `POST /artifacts/api/gc/sweep {pins}` — SKIPPED `dry run` on a dry run | artifacts.plan, pins.deployments, pins.ci, pins.dependencies, pins.images, pins.workspaces, pins.projects |
| `containers.images` | containers | `POST /containers/api/gc/images` | pins.deployments |
| `containers.volumes` | containers | `POST /containers/api/gc/volumes` | usage.before |
| `containers.build-cache` | containers | `POST /containers/api/gc/build-cache` | usage.before |
| `repos.catalogue` | projects | `GET /projects/api/repositories` | — |
| `branches.sweep` | workspaces | `POST /workspaces/api/gc/branches {dryRun, repositories, keepPrefixes}` | repos.catalogue |
| `configuration.entries` | configuration | `POST /configuration/api/gc/entries {dryRun, deployments}` | pins.deployments |
| `tags.sweep` | projects | `POST /projects/api/gc/tags {dryRun, pins}` | pins.deployments, pins.ci, pins.dependencies, pins.images, pins.workspaces, pins.projects, repos.catalogue |
| `claims.idp-clients` | deployments | `GET /deployments/api/claims/idp-clients` | — |
| `idp.service-clients` | idp | `POST /idp/api/gc/service-clients {dryRun, claims}` — FAILED, no request, when the read answered no claims | claims.idp-clients |
| `artifacts.usage.after` | artifacts | `GET /artifacts/api/store/summary` | artifacts.sweep |
| `usage.after` | containers | `GET /containers/api/gc/usage` | artifacts.sweep, containers.images, containers.volumes, containers.build-cache |

**The pins are read once and handed on.** `artifacts.plan` and `artifacts.sweep` send

```json
{"pins": {"deployments":       <the deployments answer, verbatim>,
          "ciDaemon":          <the ci answer, verbatim>,
          "dependencies":      <the maintenance answer, verbatim>,
          "configuredImages":  <the configuration answer, verbatim>,
          "workspaceLaunches": <the workspaces answer, verbatim>,
          "projectLaunches":   <the projects answer, verbatim>}}
```

qits-artifacts uses these instead of its own HTTP readers; a missing member means that source is
unanswered, which makes the plan not executable and aborts a sweep. The readers moved here because
this is the one component holding an idp client for every peer — theirs had no credential and were
401-ing.

**Four of the six sources are CONSUMPTION rather than deployment**, and that is why the first two
could not stand in for them: nothing deploys the library a pom pins, and nothing deploys the
workspace image a person's next click pulls. `pins.dependencies` is what every repository's main
still references in its manifests (qits-platform-maintenance); `pins.images` is the configured
container image versions a workspace, editor or agent launch resolves (qits-configuration). Before
they were read, all that kept either alive was how recently somebody happened to ask for it.

**Six sources because they are in six tenses.** `pins.deployments` is what serves right now,
`pins.ci` what a run launches, `pins.dependencies` what source still references, `pins.images` what
the NEXT deploy will configure — and `pins.workspaces` / `pins.projects` what a launch pulls TODAY.
That last one is the EFFECTIVE value, answered by the service that would do the pulling out of the
config it is actually running with, and it lags the configured one until that service is redeployed.
A qits-workspaces still running last week's `QITS_WORKSPACE_IMAGE_VERSION` pulls an image
qits-configuration has already moved past, and nothing but access was keeping it alive. Both ride
peers this process already drives — no new target url, no new oidc client, only a second path on a
socket that was already open.

**The registry plane is measured too, and it has its own pair.** `artifacts.usage.before` and
`artifacts.usage.after` read `GET /artifacts/api/store/summary` — `diskTotalBytes`, `ociUnionBytes`,
`docsBytes`, `sbomBytes` — because a registry blob is a row and a file in qits-artifacts' own store
and `docker system df` cannot see one of them. A run whose whole receipt was the docker read
reported a platform that was not growing while the registry did: the 2026-09-04 storage incident was
50 GB nobody's receipt showed. The after-step hangs off `artifacts.sweep` alone, so a container
prune that failed still leaves the registry's own before-and-after in the run.

**`configuration.entries` deletes rows, not bytes.** qits-configuration deletes entries of RETIRED
keys — a key some declaration of an application used to state and that no surviving declaration
states any longer, neither a serving nor a rollback version. The rule for what counts as retired
lives with qits-configuration, the owner of the store; this process hands it `pins.deployments`
verbatim, the one pin its rule needs, and that is the step's only edge — the other five pin sources
answer a different tense that has no bearing on whether a configuration key is still declared. It
frees no disk, so it hangs off neither `usage.after` plane, and it runs on a dry run too, the same
reasoning as `branches.sweep`.

**`idp.service-clients` deletes service clients nothing claims** (qits-878). qits-deployments
provisions a qits-idp client for every application declaring an `idp:client` resource, and is the
one service that knows which client ids are still claimed; `claims.idp-clients` reads them and the
sweep hands the `claims` array to qits-idp verbatim, which judges what is unclaimed, what is still
in its grace window and which client is the caller. A failed read skips the sweep (the edge); a read
that answered ZERO claims is refused in the body rather than sent, because an empty claim set would
condemn every service client — this service's own included. It runs on a dry run too (qits-idp
honours the flag) and frees no disk, so it hangs off neither `usage.after` plane. qits-idp is
reached with the same `qits` bearer as every other peer.

**`tags.sweep` decommissions git tags, on the platform's git host and its backup twin alike.**
qits-projects is the one peer that can reach both addresses, so it is the one asked, carrying the
same `{"pins": {...}}` object `artifacts.plan` sends — every one of the six sources, because a tag
can be the thing any of them protects: a sha a deployment still serves, a version a manifest
references, or an image a launch would pull. `repos.catalogue` is its iteration set, the same role
it plays for `branches.sweep`. All seven are fail-closed edges: an unread pin or an unread catalogue
skips the sweep before its body is built. It frees no disk either, so it is not a `usage.after`
dependency, and it runs on a dry run too. This is also the one call slow enough to need its own
budget: `qits.orchestrator.gc.tags-sweep-call-timeout` (15 minutes, PeerClient's four-argument
`post`) rather than the shared `qits.orchestrator.gc.call-timeout` (2 minutes) every other step
uses, because walking the whole catalogue and pushing a decommissioning commit per affected
repository to GitHub is a different order of call than one store's own read or write.

**Only what needs a pin waits for one.** `containers.build-cache` hangs off `usage.before`, not
off the image sweep: a prune has no keep-set, so a broken pin read must not cost the platform the
largest reclaim of the night. Declaration order still runs it after the image sweep.

**The image keep-set** is every deployment pin as a local tag, `qits/<applicationName>:<sha>`, plus
`qits.orchestrator.gc.image-keep-prefixes`. Fail-closed is the edge: a pin read that failed skips
the image sweep, so an empty keep-set can never reach qits-containers.

**The branch sweep is the same pin pattern, one store further out.** The repository catalogue is
the sweep's iteration set, read here because this component holds the credential, and handed to
qits-workspaces — which owns branch semantics and refuses on its own authority: the main branch,
`environment/*` and anything an active workspace stands on are never candidates, whatever the
request says. `qits.orchestrator.gc.branch-keep-prefixes` can only widen that. A failed catalogue
read skips the sweep; `usage.after` does not wait for it, because deleted refs free no docker disk.
Unlike the registry sweep, the branch sweep runs on a dry run too — qits-workspaces judges
identically and deletes nothing, so the dry figures are real figures.

**Dry run** still calls every peer — each one plans instead of deleting — so its figures are real
figures. Only `artifacts.sweep` is skipped outright, because qits-artifacts' sweep has no dry mode,
and `usage.after` still runs behind it: that skip is policy, not breakage.

### What starts a run

Three things, and none of them adds a step — the run is the same run whatever asked for it. `trigger`
on the row says which, as `manual`, `scheduled` or `event`.

| trigger | what it is | gates |
|---|---|---|
| `manual` | a person pressed Run now, or a machine posted `POST /processes/gc/runs` | none — a person is the decision, and the request carries its own `dryRun` |
| `scheduled` | the cron, 03:00 UTC (`schedule/GcSchedule`) | `gc.enabled`; deletes unless `gc.dry-run` |
| `event` | **a deployment went live** (`bus/DeploymentActiveListener` → `schedule/GcDeployTrigger`) | `gc.enabled` and `gc.deploy-trigger.enabled`; **never** dry |

**The cron is a backstop now rather than the policy.** A clock is a guess about when there is
something to collect; a deployment is the fact — it is the moment the image the predecessor container
ran, and every registry identity the release it carried superseded, stop being referenced by
anything. Until those bytes had a trigger of their own they waited for 03:00 the next day for no
reason but the schedule.

**A deployment wave is one run, not eight.** The trigger is a **trailing-edge debounce**: each
`DeploymentActive` moves the pending run to `gc.deploy-trigger.quiet-period` (PT10M) from now, so a
platform release rolling several applications collapses into ONE run that fires after the last of
them — and that run then drains what the whole wave superseded, registry identities and host images
together, in a single pass. Firing on the leading edge would plan against a platform still being
deployed and have the seven behind it refused as already-active.

**A busy executor re-arms rather than loses the wave.** `RunStore.open` refuses a second run of a
kind inside its own opening transaction; that refusal is the ordinary case for a long run, so it is a
DEBUG line and the wave is parked for another quiet period.

The bus side is one durable consumer, `orchestrator-gc-on-deploy`, initialized at the **head** of the
log — replaying from the epoch would arm a collection for every deployment this platform has ever
made. It is this service's only use of the event bus, and it publishes nothing.

## API

Under `/orchestrator/api`. Every route takes `qits:admin` (a person, via qits-gateway's
`X-Qits-User` / `X-Qits-Roles`) or `qits:system` (a machine, via a bearer). Every `GET` also takes
`qits:agent` (a commissioned agent), and so does starting a run — `gc` is the only technical process
this service knows, and a `qits:agent` caller may start one, dry-run or real, the same as the other
two roles. There is no anonymous route. `qits:admin-agent` (an ADMIN workspace's own coding-agent
container) is admitted everywhere `qits:admin` is, for now (qits-628 follow-up).

```
GET  /processes                      → [{kind, name, description,
                                         steps:[{id, name, target, dependsOn[]}]}]
GET  /processes/{kind}/runs?limit=20 → [{id, kind, trigger, dryRun, status,
                                         startedAt, finishedAt, summary}]
POST /processes/{kind}/runs {dryRun} → 202 {id}   404 unknown kind   409 one is active
GET  /runs/{id}                      → the run above, plus
                                        steps:[{id, name, target, dependsOn[], status,
                                                startedAt, finishedAt, httpStatus,
                                                request:{method, url, body},
                                                response, error, summary}]
```

- `status` is `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED` or `SKIPPED`. A run is only `RUNNING`,
  `SUCCEEDED` or `FAILED`, and it is `FAILED` if any step is — a skip does not fail it. **Two kinds
  of skip share one status**: `error: "dry run"` is the process choosing not to call, and
  `error: "skipped: <step> failed"` is the consequence of a failure. Only the second cascades; the
  `error` text is what tells them apart on the wire.
- `trigger` is `manual` or `scheduled`.
- **`request.body` and `response` are STRINGS carrying JSON text**, not embedded objects. A
  response is stored as the bytes that arrived, bounded at 1 MiB with a truncation marker past it —
  and a truncated document is not parseable JSON, so it could not live in an object-typed field. A
  client parses the string and copes with one that does not parse.
- A run's `summary` is one line per step, joined; a step's is one line read out of that peer's own
  answer.
- `POST` answers 202 and does not wait. A gc run is minutes of somebody else's pruning; the client
  polls `GET /runs/{id}` every two seconds while it is `RUNNING`.

The document is at `/orchestrator/q/openapi`, the browsable UI at `/orchestrator/q/swagger-ui`, and
readiness at `/orchestrator/q/health/ready`. **The client is served at `/` on this service's own
host, `orchestrator.<env>.<domain>`.** The machine surface keeps its segment and is path-routed on
every host, so the client's same-origin calls to `/orchestrator/api/...` are unchanged.

## Configuration

Every key below is defaulted in the domain jar (`orchestrator/src/main/resources/META-INF/microprofile-config.properties`)
and overridable by environment without a rebuild.

| key | default | what it decides |
|---|---|---|
| `qits.orchestrator.targets.artifacts-url` | `http://qits-artifacts:8080` | where qits-artifacts is |
| `qits.orchestrator.targets.containers-url` | `http://qits-containers:8080` | where qits-containers is |
| `qits.orchestrator.targets.ci-url` | `http://qits-ci:8080` | where qits-ci is |
| `qits.orchestrator.targets.deployments-url` | `http://qits-platform-deployments:8080` | where the deployer is |
| `qits.orchestrator.targets.projects-url` | `http://qits-projects:8080` | where the repository catalogue is |
| `qits.orchestrator.targets.workspaces-url` | `http://qits-workspaces:8080` | where branch semantics live |
| `qits.orchestrator.targets.maintenance-url` | `http://qits-platform-maintenance:8080` | where the dependency pins are |
| `qits.orchestrator.targets.configuration-url` | `http://qits-configuration:8080` | where the configured image pins are |
| `qits.orchestrator.targets.idp-url` | `http://${QITS_ENVIRONMENT:dev}-qits-idp:8080` | where the service-client store is |
| `qits.orchestrator.gc.enabled` | `true` | whether the CLOCK may start a run |
| `qits.orchestrator.gc.cron` | `0 0 3 * * ?` | when it does: 03:00 every day |
| `qits.orchestrator.gc.time-zone` | `UTC` | the zone the cron is read in (the platform's convention) |
| `qits.orchestrator.gc.dry-run` | `false` | whether the SCHEDULED run may delete |
| `qits.orchestrator.gc.deploy-trigger.enabled` | `true` | whether a DEPLOYMENT may start a run |
| `qits.orchestrator.gc.deploy-trigger.quiet-period` | `PT10M` | how long after the LAST deployment of a wave that run fires |
| `qits.orchestrator.gc.image-keep-prefixes` | `qits/build-images/,qits/graalvmce-musl-builder` | tag prefixes no rule may condemn |
| `qits.orchestrator.gc.branch-keep-prefixes` | `environment/` | branch prefixes the merged-branch sweep may never condemn (additive to qits-workspaces' own refusals) |
| `qits.orchestrator.gc.image-min-age` | `PT6H` | the build-then-push grace for an image |
| `qits.orchestrator.gc.volume-min-age` | `PT24H` | the stop-then-start grace for a volume |
| `qits.orchestrator.gc.build-cache-keep-bytes` | `10737418240` | what the HOST buildkit cache may keep after a prune |
| `qits.orchestrator.gc.builder-cache-keep-bytes` | `1073741824` | what a `buildx_buildkit_*` BUILDER container may keep |
| `qits.orchestrator.gc.call-timeout` | `PT120S` | how long one peer call may take |

**Two build-cache budgets, because the two caches are not the same kind of thing.** The host cache
is what every CI build warms and re-reads, so 10 GiB of it is kept — measured against a host sitting
at ~37 GB. A `buildx_buildkit_*` builder is a bootstrap-time cache: warmed once while a machine is
built, then unread until the next bootstrap, so it is pruned to 1 GiB. One number for both is what
left a 13.7 GB bootstrap builder untouched every night — it was smaller than the host budget, so a
shared keep-storage never reached it. qits-containers falls back to `keepStorageBytes` when
`builderKeepStorageBytes` is absent.

**Four tier services by configured url.** qits-artifacts, qits-containers, qits-ci and
qits-configuration are per environment (`dev-qits-artifacts`, `dev-qits-configuration`) while this
service is platform tier, so a live platform injects the qualified names. Known debt, the same one
qits-configuration carries.

**Outbound credentials** are ONE named oidc client, `qits` (epic qits-540 dossier, 'Plan (as of
2026-09-13)', C4). Its id, secret and idp address come from the deployer and nowhere else:
`.config/qits/deployments.yml` declares `resources: idp:client`, so qits-deployments provisions
this application's own qits-idp service client and injects `QITS_RESOURCE_IDP_URL`,
`QITS_RESOURCE_IDP_CLIENT_ID` and `QITS_RESOURCE_IDP_CLIENT_SECRET` (the shipped defaults are the
dev estate's `dev-qits-idp` alias and `dev-qits-orchestrator`). No configuration entry names any of
the three, and the old `QUARKUS_OIDC_CLIENT_ARTIFACTS_*` fallback is gone. A token is cut for the
PLATFORM rather than for one receiver — qits-idp puts `qits-platform` on every token it mints and
every peer accepts it — so one client and one audience serve all nine calls, and what this service
may do at a peer is decided by its roles.

One switch, `quarkus.oidc-client.qits.client-enabled`: shipped **true**, because every deployment
has the resource, and **false** under `%dev` and `%test`, so a suite or a local run never dials an
idp. Off, calls go out with the forward-auth pair alone (`X-Qits-User: qits-platform-orchestrator`,
`X-Qits-Roles: qits:system`), which every call carries regardless.

**The store** is its own PostgreSQL database, `qits_platform_orchestrator`, declared by
`resources: postgresql:db` in `.config/qits/deployments.yml` and handed over as
`QITS_RESOURCE_DB_URL` / `_USERNAME` / `_PASSWORD`. Two tables: `op_run` and `op_step`. It is the
only account of a deletion that exists on this platform — not a cache.

**A second database comes with the event bus**, `qits_platform_orchestrator_eventstream`, declared on
the same line as `postgresql:eventstream:…` and handed over as `QITS_RESOURCE_EVENTSTREAM_*`. It is
the qits-eventstream jar's outbox and claim tables, provisioned by the deployer on the next deploy.
`qits.eventstream.enabled=false` (`%dev`, `%test`) stops publishing, sweeping and dialling — never
the datasource, which Quarkus opens and migrates at boot regardless.

## Building and testing

```
./mvnw clean verify -Dquarkus.http.test-port=0
```

Green on a clone with **no docker and no credentials** — the suite spawns its own PostgreSQL from a
Maven artifact (zonky) and the peers are faked. It needs two things: a **node on PATH** and an
**initialised webui submodule**, because `verify` runs `package` and `package` is where Quinoa
builds the client. `./mvnw test` needs neither — Quinoa is off in test mode.

`-Dquarkus.http.test-port=0` is not optional on the deployment host: Quarkus' default test port
8081 is the platform's own npm registry there.

Integration tests are skipped by default. `-DskipITs=false` runs them against the fast-jar —
`PackagedSurfaceIT`, and the five **user-story** classes that drive a whole gc run against an in-JVM
stand-in for all nine peers and emit `service/target/userstories/` (see `AGENTS.md`); `-Dnative`
builds the GraalVM binary (`.sdkmanrc` names `25.0.2-graalce`) and runs it against that.

The stories reach nothing outside the JVM they run in, so they need no docker and no credentials
either. Reading them is the fastest way to see what a run actually does:

```
./mvnw verify -Dquarkus.quinoa=false -DskipITs=false \
  "-Dit.test=TokenValidationBootstrapIT,GarbageCollectionRunIT,PeerFailureIT,RunHistoryIT,DeletionRefusalIT"
open service/target/userstories/index.html
```

The image is `docker/Dockerfile`, built from the repo root with the client bundle already in the
context — see `AGENTS.md`.
