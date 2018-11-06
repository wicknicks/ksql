package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
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
import java.util.ArrayList;
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

  private KsqlEngine createKsqlEngine() {
    return TestUtils.createKsqlEngine(
        ksqlConfig,
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get);
  }

  private List<Pair<CommandId, Command>> buildCommandLog(
      final List<Pair<CommandId, Command>> commmandsSoFar,
      final String... statements) {
    final KsqlEngine ksqlEngine = executeStatements(commmandsSoFar, false);
    final List<Pair<CommandId, Command>> commands = Lists.newArrayList();
    for (final String statement : statements) {
      final List<PreparedStatement> parsed = ksqlEngine.parseStatements(statement);
      ksqlEngine.buildMultipleQueries(statement, ksqlConfig, Collections.emptyMap());
      final CommandIdAssigner assigner = new CommandIdAssigner(
          new MetaStoreImpl(new InternalFunctionRegistry()));
      commands.add(
          new Pair<>(
              assigner.getCommandId(parsed.get(0).getStatement()),
              new Command(statement, Collections.emptyMap(), Collections.emptyMap())
          )
      );
    }
    ksqlEngine.close();
    return commands;
  }

  private KsqlEngine executeStatements(
      final List<Pair<CommandId, Command>> commands, boolean close) {
    final KsqlEngine ksqlEngine = createKsqlEngine();
    final StatementParser statementParser = new StatementParser(ksqlEngine);
    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser);
    commands.forEach(
        p -> statementExecutor.handleStatement(p.getRight(), p.getLeft(), Optional.empty()));
    if (close) {
      ksqlEngine.close();
    }
    return ksqlEngine;
  }

  private KsqlEngine recoverStatements(final List<Pair<CommandId, Command>> commands) {
    final KsqlEngine ksqlEngine = createKsqlEngine();
    final StatementParser statementParser = new StatementParser(ksqlEngine);
    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser);
    final RestoreCommands restoreCommands = new RestoreCommands();
    commands.forEach(
        p -> restoreCommands.addCommand(p.getLeft(), p.getRight()));
    statementExecutor.handleRestoration(restoreCommands);
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
    // TODO: uncomment: assertThat(source.getTimestampExtractionPolicy(), equalTo(other.getTimestampExtractionPolicy()));
    assertThat(source, anyOf(instanceOf(KsqlStream.class), instanceOf(KsqlTable.class)));
    assertTopicsEqual(source.getKsqlTopic(), other.getKsqlTopic());
    if (source instanceof KsqlTable) {
      assertTablesEqual((KsqlTable) source, other);
    }
  }

  private void shouldRecoverCorrectly(final List<Pair<CommandId, Command>> commands) {
    // Given:
    final KsqlEngine engine = executeStatements(commands, true);

    // When:
    final KsqlEngine recovered = recoverStatements(commands);

    // Then:
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
    final List<Pair<CommandId, Command>> initialCommands = buildCommandLog(
        Collections.emptyList(),
        createStream("A", "A", "COLUMN STRING"),
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    final List<Pair<CommandId, Command>> commands = new ArrayList<>(initialCommands);
    commands.addAll(
        buildCommandLog(
            initialCommands,
            "TERMINATE CSAS_B_0;",
            "INSERT INTO B SELECT * FROM A;",
            "TERMINATE InsertQuery_1;"
        )
    );
    commands.addAll(
        buildCommandLog(
            initialCommands,
            "TERMINATE CSAS_B_0;"
        )
    );
    shouldRecoverCorrectly(commands);
  }

  @Test
  public void shouldRecoverDropCorrectly() {
    final List<Pair<CommandId, Command>> initialCommands = buildCommandLog(
        Collections.emptyList(),
        createStream("A", "A", "COLUMN STRING"),
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;"
    );
    final List<Pair<CommandId, Command>> commands = new ArrayList<>(initialCommands);
    commands.addAll(
        buildCommandLog(
            initialCommands,
            "INSERT INTO B SELECT * FROM A;"));
    commands.addAll(
        buildCommandLog(
            initialCommands,
            "DROP STREAM B;"));
    shouldRecoverCorrectly(commands);
  }

  @Test
  public void shouldRecoverLogWithTerminatesCorrectly() {
    final List<Pair<CommandId, Command>> initialCommands = buildCommandLog(
        Collections.emptyList(),
        createStream("A", "A", "COLUMN STRING"),
        createStream("B", "B", "COLUMN STRING")
    );
    final List<Pair<CommandId, Command>> commandsAfterInsert = new ArrayList<>(initialCommands);
    commandsAfterInsert.addAll(
        buildCommandLog(
            commandsAfterInsert,
            "INSERT INTO B SELECT * FROM A;"
        )
    );
    final List<Pair<CommandId, Command>> commands = new ArrayList<>(commandsAfterInsert);
    commands.addAll(
        buildCommandLog(
            initialCommands,
            "DROP STREAM B;"
        )
    );
    commands.addAll(
        buildCommandLog(
            commandsAfterInsert,
            "TERMINATE InsertQuery_0;"
        )
    );
    shouldRecoverCorrectly(commands);
  }
}
