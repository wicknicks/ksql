package io.confluent.ksql.connect;

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.PropertyOverrider;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseIT {

  private static final Logger log = LoggerFactory.getLogger(BaseIT.class);

  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

  protected EmbeddedConnectCluster connect;

  protected KsqlEngine ksqlEngine;
  private ServiceContext serviceContext;

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();

    // start the clusters
    connect.start();
  }

  protected void startKsql() {
    log.info("Starting ksql... ");
    KsqlConfig ksqlConfig = ksqlConfig();

    serviceContext = ServiceContextFactory.create(ksqlConfig, DisabledKsqlClient::instance);
    ksqlEngine = new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        new InternalFunctionRegistry(),
        ServiceInfo.create(ksqlConfig),
        new SequentialQueryIdGenerator()
    );
  }

  private KsqlConfig ksqlConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    return new KsqlConfig(config);
  }

  public void createKsqlStream(String sql) {
    log.info("Executing query... ");

    final ParsedStatement stmt = ksqlEngine.parse(sql).get(0);
    final PreparedStatement<?> prepared = ksqlEngine.prepare(stmt);

    ConfiguredStatement<?> cs = ConfiguredStatement.of(prepared, emptyMap(), ksqlConfig());

    ksqlEngine.execute(serviceContext, cs);
  }

  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory = Injectors.DEFAULT;

  public QueryMetadata runKsqlQuery(String sql) {
    final PreparedStatement<?> prepared = ksqlEngine.prepare(ksqlEngine.parse(sql).get(0));

    final Injector injector = injectorFactory.apply(ksqlEngine, serviceContext);
    final ConfiguredStatement<?> configured = injector.inject(ConfiguredStatement.of(
        prepared,
        emptyMap(),
        ksqlConfig()
    ));

    final CustomExecutor executor =
        CustomExecutors.EXECUTOR_MAP.getOrDefault(
            configured.getStatement().getClass(),
            (passedExecutionContext, s, props) -> passedExecutionContext.execute(
                passedExecutionContext.getServiceContext(), s));

    ExecuteResult result = executor.apply(
        ksqlEngine,
        configured,
        Collections.emptyMap()
    );

    final List<QueryMetadata> queries = new ArrayList<>();
    result.getQuery().ifPresent(queries::add);

    for (final QueryMetadata queryMetadata : queries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        queryMetadata.start();
      } else {
        queryMetadata.start();
        log.warn("Ignoring statements: {}", sql);
        log.warn("Only CREATE statements can run in KSQL embedded mode.");
      }
    }

    return queries.get(0);
  }

  @FunctionalInterface
  private interface CustomExecutor {
    ExecuteResult apply(
        KsqlExecutionContext executionContext,
        ConfiguredStatement<?> statement,
        Map<String, Object> mutableSessionPropertyOverrides
    );
  }

  @SuppressWarnings("unchecked, unused")
  private enum CustomExecutors {

    SET_PROPERTY(SetProperty.class, (executionContext, stmt, props) -> {
      PropertyOverrider.set((ConfiguredStatement<SetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    }),
    UNSET_PROPERTY(UnsetProperty.class, (executionContext, stmt, props) -> {
      PropertyOverrider.unset((ConfiguredStatement<UnsetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    }),
    QUERY(Query.class, (executionContext, stmt, props) -> {
      return ExecuteResult.of(
          executionContext.executeQuery(executionContext.getServiceContext(), stmt.cast()));
    })
    ;

    public static final Map<Class<? extends Statement>, CustomExecutor> EXECUTOR_MAP =
        ImmutableMap.copyOf(
            EnumSet.allOf(CustomExecutors.class)
                .stream()
                .collect(Collectors.toMap(
                    CustomExecutors::getStatementClass,
                    CustomExecutors::getExecutor))
        );

    private final Class<? extends Statement> statementClass;
    private final CustomExecutor executor;

    CustomExecutors(
        final Class<? extends Statement> statementClass,
        final CustomExecutor executor) {
      this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
      this.executor = Objects.requireNonNull(executor, "executor");
    }

    private Class<? extends Statement> getStatementClass() {
      return statementClass;
    }

    private CustomExecutor getExecutor() {
      return this::execute;
    }

    public ExecuteResult execute(
        final KsqlExecutionContext executionContext,
        final ConfiguredStatement<?> statement,
        final Map<String, Object> mutableSessionPropertyOverrides
    ) {
      return executor.apply(
          executionContext, statement, mutableSessionPropertyOverrides);
    }
  }

  protected void stopConnect() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  protected void stopKsql() {
    ksqlEngine.close();
    serviceContext.close();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.");
      return Optional.empty();
    }
  }

}
