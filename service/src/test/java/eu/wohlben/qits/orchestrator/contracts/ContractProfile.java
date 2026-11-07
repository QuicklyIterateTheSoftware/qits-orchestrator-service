package eu.wohlben.qits.orchestrator.contracts;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * <b>The application the contract tests record and verify against</b>: the shipped configuration,
 * unchanged — the point of the profile is the restart, not an override.
 *
 * <p>Every {@code @QuarkusTest} on the default profile shares one application and one database, and
 * {@code quarkus.flyway.orchestrator.clean-at-start} wipes that schema only when Quarkus restarts.
 * The run tables are what this service's answers are made of, so a recording on the shared database
 * would list whatever gc runs the neighbouring classes happened to leave, and the runs a state
 * seeds would be rows those classes never expected to find. A profile of its own is a restart on
 * the way in (an empty schema, so the recording sees only what its states stage) and another on the
 * way out (so the next class does not see them either).
 *
 * <p>Shared by {@link GoldenMasterRecordingTest} and {@link ConsumerPactVerificationTest}, so the
 * two boot one application between them. The identity is each class's {@code @TestSecurity}, as
 * everywhere else in the suite.
 */
public class ContractProfile implements QuarkusTestProfile {}
