# qits-platform-orchestrator — working notes

Read `README.md` first: it defines the model, lists the gc steps, the routes and the config keys.
This file is the working conventions on top of it.

## The rules that shape everything

**A process only SENDS REQUESTS.** It deletes nothing, holds no socket, opens no store but its own
run log, and makes no decision an owner has not published as an API. Every rule about what may be
deleted lives with the store's owner — qits-artifacts' GC engine, qits-containers' keep-rules. A
rule re-implemented here would be a second opinion, and it would be the copy no real deletion
exercises. If a process needs something a peer cannot do, the change goes in the peer.

**The contract is pinned by `qits-orchestrator-plan.md` in the qits-qits wrapper.** Route shapes,
step ids, the edges between them, and the request and response bodies of all six peers are written
down there and the repositories that own them build against them in parallel. Changing one of those
shapes is a plan edit and a conversation, not a commit here.

**A clone of this repo alone builds and tests green** — no monorepo, no docker, no prior `mvn
install` elsewhere, no credentials. That is why the poms duplicate versions instead of inheriting
them, why the suite spawns its own PostgreSQL from a Maven artifact, and why the peers are faked at
the client rather than stubbed with a server.

**The one thing it needs besides Maven Central** is the platform's own Maven repository, for
`qits-db-core`, `qits-auth-core` and `qits-arch-rules`. `<repositories>` in the root pom points at
`${qits.maven.repository.url}` (`https://registry.qits.wohlben.eu/artifacts/maven/maven` by
default), which answers 401 without the commissioned client; the image build overrides it through
`.qits-maven-settings.xml`, which mirrors the exact repository id `qits-maven` — an exact id match
is what gets past Maven's `external:http:*` blocker.

**The gate is `./mvnw clean verify -Dquarkus.http.test-port=0`**, and since the client landed it
needs BOTH a node on PATH and `git submodule update --init`. Always `clean` — incremental
compilation leaves stale generated classes behind when a shape changes. Port 0 is not optional on
the deployment host: 8081 is the platform's own npm registry there.

**Anything returned as `Response.entity(...)` is invisible to the build-time Jackson analysis**,
which is what `api/ApiWireReflection` exists for. The 202 from a started run is exactly such a
response. A new response type joins that list in the commit that adds it; the failure is a 500 in
the native binary while every JVM test stays green. `bus/EventWireReflection` is the second member of
that family, for the same failure one plane over: `CanonicalJson` builds its own `ObjectMapper`, so
every type the bus binds is invisible to the same analysis.

## Adding a process

One class and nothing else:

1. Implement `TechnicalProcess` as an `@ApplicationScoped` bean. `ProcessRegistry` discovers it —
   there is no list to edit, and two processes claiming one kind is a boot failure rather than a
   last-one-wins.
2. Give each step a stable `id`, a `target` from `PeerTarget`, its `dependsOn` edges, and a body
   returning a `StepResult`.
3. A body **never throws**: `StepResult.of(exchange, summary)` turns a peer's answer into the
   verdict, and `StepResult.skipped(reason)` is the other ending. The executor catches a thrown one
   anyway, but the row it writes then says less than the body could have.
4. Write the summariser beside it. It reads figures out of the peer's answer and **computes
   nothing** — a summary that re-derived what would die would be a second policy.

Its schedule, if it has one, is a class in `service/schedule` beside `GcSchedule` — and so is
anything else that starts it unattended: `GcDeployTrigger` is a class in the same package, because
"what starts a run" is one place to read whether the answer is a clock or an event.

## The executor

**One thread for the whole service**, not one per kind: a run's work is other people's disks, and
two runs overlapping would be two plans against two moments of the same stores. A second run of the
same kind is refused earlier still, by `RunStore.open`, whose active-run check is **inside** the
opening transaction — a person and the cron arriving together is the ordinary case, not a race
worth losing.

**Steps run in declaration order; the edges decide only what a failure skips.** A step whose
dependencies did not fail runs when the list reaches it, so a process reads top to bottom. A failed
step marks every transitive dependent SKIPPED, and the error names the step that actually FAILED
rather than the skipped neighbour in between — a reader should not have to walk the graph backwards.
Independent steps still run: a night where one broken peer stopped every unrelated reclaim would be
a night of no reclaim for no reason.

**Two kinds of skip, and only one of them is contagious.** `SkipKind.POLICY` is the process choosing
not to call — a dry run does not sweep — and it is a SATISFIED dependency: what comes after it runs.
`SkipKind.FAILURE` is the executor's own, written when a dependency failed, and it cascades. Both go
out as `status: SKIPPED` with a sentence in `error`; the kind is read once, during the run, and is
deliberately neither on the wire nor in a column. `blocker()` consults exactly one map — a step is
in it when it FAILED or was FAILURE-skipped, and absent otherwise.

This was measured, not designed: the platform's first real dry run answered 200 to all nine calls
and still reported `usage.after` as `skipped: artifacts.sweep failed`, because the executor read
every non-SUCCEEDED dependency as broken. A green run that reads as broken is worse than a red one.
**A new policy skip returns `StepResult.skipped(reason)`; nothing but the executor may produce a
failure skip.**

**A SKIPPED step does not fail a run.** A skip is either the consequence of a failure already
counted, or not a failure at all.

**`execute` never throws.** A thrown exception on the worker would leave a RUNNING row nothing can
close, and no second run of that kind would ever be allowed again.

## Fail-closed is an edge, not an `if`

Nothing deletes against a keep-set it could not read, and the mechanism is the dependency, not a
check inside a step. `artifacts.plan` and `artifacts.sweep` depend on all six pin reads and
`containers.images` on the deployment one, so a failed pin read skips what it protects before a body
runs. `GcProcess.imagesBody` would build
an empty `keep` from an unreadable pin answer — and never gets the chance to. Keep both: the edge is
the guarantee, the empty-set path is the belt.

**The rule cuts the other way too: a step with no keep-set must not wait on a pin.**
`containers.build-cache` and `containers.volumes` hang off `usage.before` alone, because a prune and
a dangling-volume sweep have nothing a pin could protect — and the build cache is the larger half of
the measured problem, so skipping it on a pin failure would cost the platform the night's biggest
reclaim for a reason that does not apply to it. Declaration order is what still runs the prune after
the image sweep; an edge would have been ordering dressed up as a requirement.

If a new step deletes on the strength of a pin, **give it the pin's edge in the same commit** — and
if it does not, **do not**.

## Outbound calls

`PeerClient` is the one way this service touches another. Two credentials on every call and they are
not alternatives: the forward-auth pair (`X-Qits-User: qits-platform-orchestrator`,
`X-Qits-Roles: qits:system`) always, and a bearer where the `qits` oidc client is enabled.

**Nothing throws.** A name that does not resolve, a timeout, a body that is not JSON — each comes
back as a `PeerAnswer` carrying the sentence, because a step's job is to record what happened and a
process whose steps had to catch would put half its outcomes on a path nobody reads.

**A token that cannot be minted is empty, not an exception** — the deployer's stance. The refusal
that matters belongs to the call: an anonymous call to a guarded peer comes back 401 and the step
records the url and the status, which is more useful than a mint failure one layer earlier.

**One named client, `qits`, for all eight peers** (service-client-identity-plan.md, C4). A token is
cut for the PLATFORM and not for one receiver: qits-platform-idp puts `qits-platform` on every token
it mints and every peer accepts it, so one client and one audience serve all eight calls and the
roles decide what this service may do at each. The client is turned on or off as a whole, so
qits-platform-maintenance and qits-configuration are reached with a bearer like the other six.

**Every other client block is injected by nothing, and each is deliberate.** The unnamed default
client, which the extension creates whether or not anything injects it and which is disabled so a
client with no auth-server-url cannot fail the boot. `artifacts`, whose env names the `qits`
client's fallbacks read and whose shipped `discovery-enabled=false` /
`early-tokens-acquisition=false` keep a deployment's
`QUARKUS_OIDC_CLIENT_ARTIFACTS_CLIENT_ENABLED=true` inert; it goes once this repository declares
`resources: idp:client` (C5) and those extras are off every deployment. And `ci`, `containers`,
`deployments`, `projects`, `workspaces`, which exist only because the deployment still sets a
`QUARKUS_OIDC_CLIENT_<NAME>_*` family for each. **A client block is not gone just because this
repository deleted it.** SmallRye discovers `quarkus.oidc-client.<name>` map keys from the
ENVIRONMENT, so one variable of a family mints the name, and a name with no block behind it takes
the extension's defaults — `client-enabled` true and `discovery-enabled` true. Such a client is
built during runtime init, before the listener accepts, and blocks on metadata discovery for
`connection-timeout` per client: an issuer that accepts and does not answer fails this service's
boot. `client-enabled=false` cannot fix that alone (the environment's `true` outranks this file);
`discovery-enabled=false` plus a `token-path` is what does, and those lines go only after the
matching entries are deleted from the deployment.

**A response is bounded at 1 MiB** with a marker appended, cut on a character boundary. An artifacts
plan lists every condemned identity on the platform; the store here is a log a person reads, and an
unbounded column would let one peer's verbosity decide this service's disk.

## Telemetry

`quarkus-opentelemetry` exports traces, logs and metrics to qits-observability. One key names the
receiver — `qits.observability.url`, scheme/host/port and no path, the shape the peer urls already
have — and the endpoint is derived from it; the protocol is `http/protobuf` because the receiver does
not speak the gRPC default. The SDK is dark in `%dev`/`%test`, the same policy as the event bus.

**The four `quarkus.otel.logs.*` keys are spelled out even where they are Quarkus' own defaults.**
That integration is still labelled PREVIEW, and it is the whole of what ships this service's logs, so
a changed default would end log export with a green build and no error anywhere; written down, it is
a diff instead. `quarkus.otel.logs.level=INFO` is the one non-default — the outbound floor, which is
what keeps a gc run's account (the schedule and the deploy trigger starting one, the executor's line
per finished run) exported and `GcDeployTrigger`'s debounce bookkeeping at DEBUG local.

`telemetry/OtelLogConfigTest` is the drift guard and asserts configuration only; that a logger call
really becomes an exported record is proven once, in qits-events. `quarkus.application.name` is the
telemetry source key — it follows the deployed identity, so do not touch it.

## The event bus

`service/…/bus/` is the whole of the bus's **SEAMS**. The machinery is the published
`qits-eventstream` jar; its rules live in that library's own repository and are not restated here.
This service **consumes one event and publishes none**.

- **`DeploymentActiveListener` is the gc run's second unattended trigger**, and it is the only reason
  this repository is on the bus at all. A deployment going live is the moment the images and registry
  identities it superseded stop being referenced; the cron is a guess about when that has happened.
  `schedule/GcDeployTrigger` is where every decision behind it sits — the trailing-edge debounce, the
  two gates, never a dry run, and what a busy executor means — and the listener is decode-and-hand-over.
- **The payload is a LOCAL record.** qits-deployments publishes `qits-platform-deployments-events`
  and the platform's Maven registry serves nothing under that coordinate, so a dependency on it is a
  release pipeline that cannot build. `CanonicalJson.payloadTo` into a record of our own is the
  platform's standing answer (qits-projects' listener of the same event is the model). The cost is
  that a rename over there is silent here.
- **`consumerId()` is `orchestrator-gc-on-deploy` and it is STORAGE.** It keys every `consumed_event`
  row and the watermark. Change it and you mint a brand-new consumer that initializes at the head of
  the log. It initializes at the **head** deliberately (`replayFromEpoch()` left false): from the
  epoch it would arm a collection for every deployment this platform has ever made.
- **Nothing here throws.** `onFrame` runs inside the transaction that claims the event, so a throw
  rolls the claim back and the event is owed forever. A payload that will not read is poison — the
  same bytes on every later offer — so it is a WARN and a return. And the run itself never starts on
  that thread: arming a timer is memory, and the run opens on the timer's thread, well outside the
  claim. A gc run inside an event's claim would hold both the transaction and the watermark for
  minutes of other people's deleting.
- **The jar brings a MANDATORY deployment resource.** `.config/qits/deployments.yml` declares
  `postgresql:eventstream:qits_platform_orchestrator_eventstream`, and the resource **name** is
  load-bearing because the jar reads `QITS_RESOURCE_EVENTSTREAM_*`. `qits.eventstream.enabled=false`
  (`%dev`, `%test`) stops publishing, sweeping and dialling — never the datasource, which Quarkus
  opens and migrates at boot regardless. That is why the suite hands out a second database
  (`testdb/EmbeddedPgConfigSource`) and why `PackagedSurfaceIT`'s profile supplies a second triple
  and the darkening key: a launched artifact runs in `NORMAL` mode, where `%test` reaches nothing.
- **`bus/EventWireReflection` is the native-image registration.** Read its javadoc before adding a
  wire type; the failure it prevents is invisible to every JVM test by construction.

## Persistence

`RunStore` is the only writer. **Every write is a `DbRetry.inNewTx` ending in a flush**: `inNewTx`
owns the transaction boundary, which is the only way a retry can tell "the body threw, so it
certainly never committed" from "the transaction manager reported it" — Narayana spells a lost
commit and a real rollback with the same exception, and the flush keeps a lost connection on the
body's side of that line. These writes happen while a deletion is in flight on another host.

**Every method is `@ActivateRequestContext`**, because the caller is usually the executor's worker
thread and a Hibernate session is bound to that context. A route's call already has one, so the
annotation covers both callers.

**Steps are written as they transition**, never at the end of the run — the client polls a RUNNING
run every two seconds.

**A run that dies mid-flight keeps `finished_at` null forever**, and nothing backfills it at boot: a
successor process knows nothing about what the dead one's calls achieved.

Schema changes go in `orchestrator/src/main/resources/db/orchestrator/migration/`, hand-written, its
own lineage on its own datasource. Keep appending, never edit an applied migration. The suites run
every migration against an empty schema, so a backfill needs a test that migrates to the version
before, writes the rows the old code wrote, and migrates the rest of the way.

## Identity: two tracks, one set of roles

A request with no `Authorization` header is USER traffic — qits-gateway performed the login and
asserted `X-Qits-User` / `X-Qits-Roles`. A request WITH a bearer is MACHINE traffic, validated by
quarkus-oidc against qits-platform-idp.

**Both land as roles, which is why every route is `@RolesAllowed({"qits:admin", "qits:system"})`.**
An operator presses Run now in a browser; a machine may post the same run. There is no anonymous
route here and there must never be one — the write surface starts deletions on other services'
stores. `stories.refusals` is where each door is shown being shut rather than described.

`quarkus.oidc.tenant-enabled=${qits.auth.machine.required:false}` — validation follows the rollout
gate rather than standing on its own, so with the gate off there is no OIDC tenant, nothing fetches
a JWKS, and a clone-alone build needs no issuer. There is no third state.

**`quarkus.oidc.token.audience` is the literal `qits-platform` and there is only that one audience.**
qits-platform-idp puts it on every token it mints, whatever the client asked for, so a peer's bearer
and a person's `qits` CLI token both address this service by it and the roles decide the rest. It is
NOT spelled from `qits.auth.machine.audience`: that key is qits-auth-core's gate contract, which the
library's startup check reads, and a tenant following an environment override of it would refuse
every token the platform actually mints. A refusal story therefore mints an audience from outside
the platform — a sibling's bearer is admitted here.

## Tests

- App-level config lives in `service/src/main/resources/application.properties` and **the tests
  inherit it** — Quarkus merges the test resources over it rather than replacing it. Never
  re-declare an app-level setting in test resources.
- **No dev services and no containers, ever.** `EmbeddedPg` starts zonky's postgres and
  `EmbeddedPgConfigSource` hands its coordinates to every `@QuarkusTest` at an ordinal above
  `application.properties`, because the port is chosen at run time. Both are **copied** per module
  rather than shared: a test-jar dependency between two modules that have none is the higher price.
  Each module names its own database.
- **`FakePeers` is an `@Alternative` over `PeerClient.get` / `.post`** — replacing those two is
  replacing the network, at no port and no dependency, and the urls in the assertions stay the real
  ones because the inherited `url()` still resolves them from the shipped target configuration.
  `PeerClientTest` covers the other half against a loopback `HttpServer`: the headers, the 1 MiB
  bound, an unreachable peer.
- **`nextPoll()` clears the session before each poll, and it is load-bearing.** A `@QuarkusTest`
  holds ONE request context for the whole test method, so one Hibernate session serves every read
  and would answer every poll from its first-level cache — the run row would look RUNNING forever
  while the worker closed it in another session. In a deployment each poll is a fresh request.
- **The scheduler is off in the suite** (`quarkus.scheduler.enabled=false`). A cron would fire only
  at 03:00, and a suite whose outcome depends on the wall clock fails once a year for nobody's
  reason.
- **`ManualDeployTriggerTimer` is the same trick for the OTHER unattended trigger**, an
  `@Alternative` over `DeployTriggerTimer` — the one seam `GcDeployTrigger` schedules through — so
  "the quiet period lapsed" is a method call. That interface exists for the test and says so: a
  debounce's whole claim is about WHEN, and shortening the period to milliseconds does not make the
  assertions deterministic, it makes them depend on thread scheduling. It is enabled suite-wide,
  which also means no stray event can ever start a run.
  <br>**Read every fake through a METHOD, never a public field.** These are normal-scoped beans, so a
  test injects a client proxy and a proxy does not forward field access — an assertion on a public
  list reads the proxy's own empty one and passes vacuously. Measured here on the first run of this
  suite.
- **The bus is dark and has a database anyway.** `qits.eventstream.enabled=false` comes from main's
  `%test`; the second datasource still opens and migrates, so `testdb/EmbeddedPgConfigSource` names
  `orchestrator_svc_eventstream` beside the run log's database. `catchup-at-startup=false` is the
  belt: the startup sweep is not a scheduler tick and `quarkus.scheduler.enabled=false` does not
  cover it.
- A `@QuarkusTest` runs under the `test` profile, where qits-auth-core ships a dev user carrying
  `qits:admin` and `qits:system` — so the shipped `@RolesAllowed` pair is exercised rather than
  bypassed, with no `@TestSecurity` fabricating an identity no deployment produces.
- **Every claim about the ARTIFACT is an IT**, and there are two kinds. `PackagedSurfaceIT` is the
  gate-off one: it hands the process `QITS_RESOURCE_DB_*` rather than restating the datasource keys,
  so the jar's own `${…}` indirection is under test, it reads the rows back over JDBC, and **its
  peers are real calls to a dead loopback port** — the honest end-to-end proof that a failure reaches
  a readable row with none of the suite's fakes involved. It is also the only place the SPA is real.
  The five **story** ITs are the gate-ON ones; see below. ITs are skipped by default;
  `-DskipITs=false` runs against the fast-jar and `-Dnative` against the binary.
- **`PackagedSurfaceIT.PackagedUnderTarget` is the profile base, and its `databaseUrl(property,
  database)` is `protected` on purpose.** What a launched process needs in order to boot at all —
  the mandatory triple, and the system-property parking that carries it across the two classloaders
  a test profile is instantiated in — is one answer. A subclass brings its own database NAME and
  never a second copy of the mechanism.

## The userflow stories

`service/src/test/java/.../stories/` is this repository's **user-story catalogue**: ten stories in
five categories, written with the `qits-userflows` framework, emitting
`service/target/userstories/` — steps, a description a person can read, and a **network diagram that
is observed rather than narrated**. The userflow half of `.config/qits/ci-event-release-request.yml`
runs them on every release-request fold and publishes the bundle as docs. They are proofs first and
documentation second; nothing in them is drawn by hand.

| class | category | what it says |
|---|---|---|
| `api.TokenValidationBootstrapIT` | authentication | the gate on, the JWKS fetched at boot, the plan served to a peer's bearer — and the three refusals on the cheapest read |
| `stories.collection.GarbageCollectionRunIT` | garbage collection | a run end to end against six peers; a dry run; one run at a time |
| `stories.faults.PeerFailureIT` | resilience | a pin read that answered 503, what it skipped and what it did not |
| `stories.operations.RunHistoryIT` | operations | the plan before a run, the account after it |
| `stories.refusals.DeletionRefusalIT` | refusals | the doors on the write surface, and what is not there |

**ONE `@TestProfile` for all five**, `stories.support.StoryProfile`, so they are one launched
process: one boot, one JWKS fetch, one database, one executor. Two profiles would be two
orchestrators and a diagram whose startup traffic landed in whichever one happened to be running.

**Class order is load-bearing and the package names carry it.** `UserflowClassOrderer` is a
topological sort over `@UserflowRunsAfter` with ties broken by fully-qualified class name, so `api.*`
runs before `stories.*` and inside `stories` it is `collection` → `faults` → `operations` →
`refusals`. Two cumulative recordings are attributed by a cursor and therefore land in whichever
story drains FIRST: the **startup JWKS fetch** (owned by the boot story) and the **outbound token
mint**, which quarkus-oidc-client caches for the hour the stub's `expires_in` grants, so it happens
on the first peer call of the whole catalogue and belongs to the first run story. Both are asserted
where they land. Rename a package and you re-attribute them.

**The far side is `stories.support.StoryPeers`** — one `com.sun` HTTP server impersonating all six
peers plus the idp token endpoint, told apart by **path prefix**, recording `METHOD PATH STATUS` to
`target/story-peers/access.log`, which the framework drains as a source. A process that only sends
requests leaves its evidence nowhere else. Answers are a pure function of the request, with **one
exception**: `refuse(prefix)` is a file the fault story writes and clears in a `finally`, because a
gc run's ten paths are fixed by `GcProcess.steps()` and "qits-ci is down tonight" cannot be spelled
as a url a story addresses. A file rather than a static field because the stub is started by the test
profile, which a launched-artifact run instantiates in a different classloader from the story's.

**NORMAL mode is the whole point.** The profile sets `qits.auth.machine.required=true`, which no
other suite here does; a launched artifact has no `%test` dev user, so an anonymous request really is
anonymous and every refusal is a claim only these tests can make. People arrive as
`X-Qits-User`/`X-Qits-Roles`, machines as MockIdp-minted bearers.

**What the stories cannot reach, stated rather than hidden:**

- **the SCHEDULED trigger.** The clock stays off (`quarkus.scheduler.enabled=false`, inherited): a
  cron at 03:00 would start an unattended run out of a CI JVM against peers that now answer, and a
  recording could not tell its ten calls from a story's. Every run in the catalogue is `manual`, and
  `GcSchedule`'s own gates — `gc.enabled`, `gc.dry-run`, the `SKIP` on concurrent execution, the
  swallowed `RunAlreadyActiveException` — have **no test in this repository at all**. That is a real
  gap, not a story's blind spot.
- **a run against real infrastructure.** No docker socket, no registry, no second postgres: what
  each peer would actually delete is that peer's own repository's claim. What is proved here is the
  request that went out, the answer that came back and the row it became.
- **the SPA.** Quinoa is off in the userflow pipeline, so `PackagedSurfaceIT` keeps every claim
  about the client.

**Editing rules.** A story class installs the taps with `StoryNetwork.install()` in `@BeforeAll` and
must pin at least one edge, or a later edit could silently empty every diagram in the class. A story
that starts a run must **await its closure** before returning — the peers' recording is drained at
story end and an in-flight run would put its remaining calls in the next story's diagram. Every
`assertEdgeCount` in the catalogue is exact: adding a request to a story means updating its count,
and that is the assertion doing its job. Labels drop the query string, so two routes differing only
in their query are ONE edge.

## The client

`service/src/main/webui` is the `qits-orchestrator-platform-frontend` submodule (`ignore = all`,
`update = merge`, `branch = main` — the sibling shape). Quinoa 2.8.2 is pinned by hand in the root
pom, because Quinoa is in no BOM and its version does not track the platform's.

- **The segment is spelled twice**, `quarkus.quinoa.ui-root-path` here and `baseHref` in the
  submodule's `angular.json`. A mismatch serves a page whose every asset 404s and nothing on this
  side notices, so `PackagedSurfaceIT` asserts the `<base href>` string rather than the status.
- **`ignored-path-prefixes` values are RELATIVE**, matched after `ui-root-path` is stripped: `/api`
  and `/q`, never `/orchestrator/api`. An absolute value matches nothing and is indistinguishable
  from an unset key. Setting the key REPLACES Quinoa's derivation, which is why both are spelled by
  hand. **Add a literal route under `/orchestrator` and its entry here in the same commit** — and
  give it a segment of its own, because an entry protects a segment and not a string prefix.
- **The bundle is built OUTSIDE the docker build.** `@qits/ui-components` exists only on the
  platform's own npm registry, which a `RUN` reaches by no address at all. So
  both `.config/qits/ci-event-release.yml` and the QA pipeline's gating half build it in the step
  container (on qits-net), and the Dockerfile neuters Quinoa's install/ci/build commands with
  `--version`, guards the staged bundle
  with a `test -f` before the multi-minute native compile, and `cp`s the bundle onto itself so
  Quinoa's MOVE does not hit overlayfs' EXDEV.
- **Quinoa is off in test mode and stays off.** Every claim about the SPA belongs in
  `PackagedSurfaceIT`.
- **`quarkus-undertow` must never be on the classpath.** It arrives transitively from anything
  servlet-shaped and takes over the static-resource route Quinoa serves the bundle through — a
  packaged process that answers the API correctly and the SPA with a 404.

      ./mvnw -pl service -am dependency:tree | grep -i undertow

## Deliberately not here yet

Each is a decision, not an omission:

- **Cancelling a run.** There is no route and no column for it. A step is one HTTP call to a peer
  that is already deleting; interrupting the wait would abandon the answer rather than the work, and
  a run row saying CANCELLED would be a claim this service cannot make.
- **PUBLISHING an event.** The `qits-eventstream` dependency arrived on 2026-09-05 for the
  *consuming* half — see "The event bus" — and nothing here announces anything. A run's start, its
  verdict and what it deleted are all readable on the API, nothing subscribes to any of them today,
  and a publisher would arrive with the vocabulary jar every announcing service has. `EventEnvelope`
  is already registered for reflection against that day.
- **Causation stamping.** `op_run` and `op_step` carry no `causation_id`, so a run started by a
  deployment does not record which one. The event id is in reach on the frame and the column is a
  migration, but a run is caused by a WAVE rather than by one event — the debounce is what makes the
  trigger correct and what makes a single parent a lie.
- **A size budget for the artifacts GC.** L1.4 of the storage plan; it is a policy the artifacts
  engine has to hold, not a number this service could pass.
- **A committed `docs/openapi.yml`.** The document is served at `/orchestrator/q/openapi`.
