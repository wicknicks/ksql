package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

public class RecoveryTest {
  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER
      = EmbeddedSingleNodeKafkaCluster.build();

  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
          "bootstrap.servers", CLUSTER.bootstrapServers()
      )
  );

  private Command createCommand(final String text) {
    return new Command(text, Collections.emptyMap(), Collections.emptyMap());
  }

  private String createStream(final String name, final String topic, final String... columns) {
    return String.format(
        "CREATE STREAM %s (%s) WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='JSON');",
        name, String.join(", ", columns), topic);
  }

  private KsqlEngine executeStatements(final List<Pair<CommandId, Command>> commands) {
    final KsqlConfig ksqlConfig = new KsqlConfig(this.ksqlConfig.originals());
    final KsqlEngine ksqlEngine = TestUtils.createKsqlEngine(
        ksqlConfig,
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get);
    final StatementParser statementParser = new StatementParser(ksqlEngine);
    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser);
    commands.forEach(
        p -> statementExecutor.handleStatement(p.getRight(), p.getLeft(), Optional.empty()));
    ksqlEngine.close();
    return ksqlEngine;
  }

  private KsqlEngine recoverStatements(final List<Pair<CommandId, Command>> commands) {
    final KsqlConfig ksqlConfig = new KsqlConfig(this.ksqlConfig.originals());
    final KsqlEngine ksqlEngine = TestUtils.createKsqlEngine(
        ksqlConfig,
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get);
    final StatementParser statementParser = new StatementParser(ksqlEngine);
    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser);
    final RestoreCommands restoreCommands = new RestoreCommands();
    commands.forEach(
        p -> restoreCommands.addCommand(p.getLeft(), p.getRight()));
    statementExecutor.handleRestoration(restoreCommands);
    ksqlEngine.close();
    return ksqlEngine;
  }

  private void assertTopicsEqual(final KsqlTopic topic, final KsqlTopic otherTopic) {
    assertThat(topic.getTopicName(), equalTo(otherTopic.getTopicName()));
    assertThat(topic.getKafkaTopicName(), equalTo(otherTopic.getKafkaTopicName()));
    assertThat(
        topic.getKsqlTopicSerDe().getClass(),
        equalTo(otherTopic.getKsqlTopicSerDe().getClass()));
  }

  private void assertTablesEqual(final KsqlTable table, final StructuredDataSource other) {
    assertThat(other, instanceOf(KsqlTable.class));
    final KsqlTable otherTable = (KsqlTable) other;
    assertThat(table.isWindowed(), equalTo(otherTable.isWindowed()));
  }

  private void assertSourcesEqual(
      final StructuredDataSource source,
      final StructuredDataSource other) {
    assertThat(source.getClass(), equalTo(other.getClass()));
    assertThat(source.getName(), equalTo(other.getName()));
    assertThat(source.getDataSourceType(), equalTo(other.getDataSourceType()));
    assertThat(source.getSchema(), equalTo(other.getSchema()));
    assertThat(source.getSqlExpression(), equalTo(other.getSqlExpression()));
    assertThat(source.getTimestampExtractionPolicy(), equalTo(other.getTimestampExtractionPolicy()));
    assertThat(source, anyOf(instanceOf(KsqlStream.class), instanceOf(KsqlTable.class)));
    assertTopicsEqual(source.getKsqlTopic(), other.getKsqlTopic());
    if (source instanceof KsqlTable) {
      assertTablesEqual((KsqlTable) source, other);
    }
  }

  private void shouldRecoverCorrectly(final List<Pair<CommandId, Command>> commands) {
    final KsqlEngine engine = executeStatements(commands);
    final KsqlEngine recovered = recoverStatements(commands);

    assertThat(
        engine.getMetaStore().getAllStructuredDataSourceNames(),
        equalTo(recovered.getMetaStore().getAllStructuredDataSourceNames()));
    for (final String sourceName : engine.getMetaStore().getAllStructuredDataSourceNames()) {
      assertSourcesEqual(
          engine.getMetaStore().getSource(sourceName),
          recovered.getMetaStore().getSource(sourceName)
      );
    }
    final Collection<PersistentQueryMetadata> queries = engine.getPersistentQueries();
    final Collection<PersistentQueryMetadata> recoveredQueries = recovered.getPersistentQueries();
    assertThat(queries.size(), equalTo(recoveredQueries.size()));
    final Map<QueryId, PersistentQueryMetadata> recoveredQueriesById
        = recoveredQueries.stream().collect(
            Collectors.toMap(PersistentQueryMetadata::getQueryId, pqm -> pqm));
    for (final PersistentQueryMetadata query : queries) {
      assertThat(recoveredQueriesById.keySet(), contains(query.getQueryId()));
      final PersistentQueryMetadata recoveredQuery = recoveredQueriesById.get(query.getQueryId());
      assertThat(query.getSourceNames(), equalTo(recoveredQuery.getSourceNames()));
      assertThat(query.getSinkNames(), equalTo(recoveredQuery.getSinkNames()));
    }
  }

  @Test
  public void shouldRecoverLogWithRepeatedTerminatesCorrectly() {
    shouldRecoverCorrectly(
        ImmutableList.of(
            new Pair<>(
                new CommandId(Type.STREAM, "A", Action.CREATE),
                createCommand(
                    createStream("A", "A", "COLUMN STRING"))),
            new Pair<>(
                new CommandId(Type.STREAM, "B", Action.CREATE),
                createCommand("CREATE STREAM B AS SELECT * FROM A;")),
            new Pair<>(
                new CommandId(Type.TERMINATE, "B", Action.EXECUTE),
                createCommand("TERMINATE CSAS_B_0;")),
            new Pair<>(
                new CommandId(Type.STREAM, "B", Action.CREATE),
                createCommand("INSERT INTO B SELECT * FROM A;")),
            new Pair<>(
                new CommandId(Type.TERMINATE, "B", Action.EXECUTE),
                createCommand("TERMINATE InsertQuery_1;")),
            new Pair<>(
                new CommandId(Type.TERMINATE, "B", Action.EXECUTE),
                createCommand("TERMINATE CSAS_B_0;"))
        )
    );
  }

  @Test
  public void shouldRecoverLogWithTerminatesCorrectly() {
    shouldRecoverCorrectly(
        ImmutableList.of(
            new Pair<>(
                new CommandId(Type.STREAM, "A", Action.CREATE),
                createCommand(
                    createStream("A", "A", "COLUMN STRING"))),
            new Pair<>(
                new CommandId(Type.STREAM, "B", Action.CREATE),
                createCommand(
                    createStream("B", "B", "COLUMN STRING"))),
            new Pair<>(
                new CommandId(Type.STREAM, "A", Action.CREATE),
                createCommand("INSERT INTO B SELECT * FROM A;")),
            new Pair<>(
                new CommandId(Type.STREAM, "B", Action.DROP),
                createCommand("DROP STREAM B;")
            ),
            new Pair<>(
                new CommandId(Type.TERMINATE, "InsertQuery_0", Action.EXECUTE),
                createCommand("TERMINATE InsertQuery_0;")
            )
        )
    );
  }
}
