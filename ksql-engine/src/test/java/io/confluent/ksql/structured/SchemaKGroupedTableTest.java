/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.execution.ExecutionStep;
import io.confluent.ksql.structured.execution.ExecutionStepProperties;
import io.confluent.ksql.structured.execution.TableAggregate;
import io.confluent.ksql.structured.execution.TableSource;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.ExecutorUtil.Function;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedTableTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private final Schema schema = SchemaBuilder.struct()
      .field("GROUPING_COLUMN", Schema.OPTIONAL_STRING_SCHEMA)
      .field("AGG_VALUE", Schema.OPTIONAL_INT32_SCHEMA)
      .build();
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");

  private KTable kTable;
  private KsqlTable<?> ksqlTable;

  @Mock
  private ExecutionStep mockExecutionStep;
  @Mock
  private ExecutionStepProperties mockExecutionStepProperties;
  @Mock
  private KGroupedTable mockKGroupedTable;
  @Mock
  private MaterializedFactory materializedFactory;

  @Before
  public void init() {
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder
        .table(ksqlTable.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String()
            , ksqlTable.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(
                ksqlTable.getSchema(),
                new KsqlConfig(Collections.emptyMap()),
                false,
                MockSchemaRegistryClient::new,
                "test",
                processingLogContext)));

  }

  private ExecutionStep buildSourceStep(final Schema schema, final KsqlTable ksqlTable) {
    return new TableSource(
        new ExecutionStepProperties(
            "source",
            schema,
            (Field) ksqlTable.getKeyField().orElse(null)
        ),
        ksqlTable.getKafkaTopicName()
    );
  }

  private SchemaKGroupedTable buildSchemaKGroupedTableFromQuery(
      final String query,
      final String...groupByColumns
  ) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(query, metaStore);
    final SchemaKTable initialSchemaKTable = new SchemaKTable<>(
        kTable,
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        queryContext.getQueryContext(),
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), ksqlTable));
    final List<Expression> groupByExpressions =
        Arrays.stream(groupByColumns)
            .map(c -> new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("TEST1")), c))
            .collect(Collectors.toList());
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKTable.getSchema(),
        null,
        false,
        () -> null,
        "test",
        processingLogContext);
    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        rowSerde, groupByExpressions, queryContext);
    Assert.assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    return (SchemaKGroupedTable)groupedSchemaKTable;
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTableFromQuery(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final WindowExpression windowExpression = new WindowExpression(
        "window", new TumblingWindowExpression(30, TimeUnit.SECONDS));
    try {
      kGroupedTable.aggregate(
          new KudafInitializer(1),
          Collections.singletonMap(
              0,
              functionRegistry.getAggregate("SUM", Schema.OPTIONAL_INT64_SCHEMA)),
          Collections.singletonMap(0, 0),
          windowExpression,
          new KsqlJsonTopicSerDe().getGenericRowSerde(
              ksqlTable.getSchema(),
              ksqlConfig,
              false,
              () -> null,
              "test",
              processingLogContext),
          queryContext
      );
      Assert.fail("Should fail to build topology for aggregation with window");
    } catch(final KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo("Windowing not supported for table aggregations."));
    }
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTableFromQuery(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    try {
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = new HashMap<>();
      aggValToFunctionMap.put(
          0, functionRegistry.getAggregate("MAX", Schema.OPTIONAL_INT64_SCHEMA));
      aggValToFunctionMap.put(
          1, functionRegistry.getAggregate("MIN", Schema.OPTIONAL_INT64_SCHEMA));
      kGroupedTable.aggregate(
          new KudafInitializer(1),
          aggValToFunctionMap,
          Collections.singletonMap(0, 0),
          null,
          new KsqlJsonTopicSerDe().getGenericRowSerde(
              ksqlTable.getSchema(),
              ksqlConfig,
              false,
              () -> null,
              "test",
              processingLogContext),
          queryContext
      );
      Assert.fail("Should fail to build topology for aggregation with unsupported function");
    } catch(final KsqlException e) {
      Assert.assertThat(
          e.getMessage(),
          equalTo(
              "The aggregation function(s) (MAX, MIN) cannot be applied to a table."));
    }
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable(
      final KGroupedTable kGroupedTable,
      final MaterializedFactory materializedFactory) {
    when(mockExecutionStep.getProperties()).thenReturn(mockExecutionStepProperties);
    when(mockExecutionStepProperties.getSchema()).thenReturn(schema);
    when(mockExecutionStepProperties.getKey()).thenReturn(Optional.of(schema.fields().get(0)));
    return new SchemaKGroupedTable(
        kGroupedTable,
        ksqlConfig,
        functionRegistry,
        materializedFactory,
        mockExecutionStep
    );
  }

  @Test
  public void shouldUseMaterializedFactoryForStateStore() {
    // Given:
    final Serde<GenericRow> valueSerde = mock(Serde.class);
    final Materialized materialized = MaterializedFactory.create(ksqlConfig).create(
        Serdes.String(),
        valueSerde,
        StreamsUtil.buildOpName(queryContext.getQueryContext()));
    when(materializedFactory.create(any(), any(), anyString())).thenReturn(materialized);
    final KTable mockKTable = mock(KTable.class);
    when(mockKGroupedTable.aggregate(any(), any(), any(), any())).thenReturn(mockKTable);
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    groupedTable.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        valueSerde,
        queryContext);

    // Then:
    verify(materializedFactory).create(
        any(Serdes.String().getClass()),
        same(valueSerde),
        eq(StreamsUtil.buildOpName(queryContext.getQueryContext()))
    );
    verify(mockKGroupedTable).aggregate(any(), any(), any(), same(materialized));
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    final Serde<GenericRow> valueSerde = mock(Serde.class);
    final Materialized materialized = MaterializedFactory.create(ksqlConfig).create(
        Serdes.String(),
        valueSerde,
        StreamsUtil.buildOpName(queryContext.getQueryContext()));
    when(materializedFactory.create(any(), any(), anyString())).thenReturn(materialized);
    final KTable mockKTable = mock(KTable.class);
    when(mockKGroupedTable.aggregate(any(), any(), any(), any())).thenReturn(mockKTable);
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);
    final Map<Integer, KsqlAggregateFunction> functions = mock(Map.class);
    final Map<Integer, Integer> columns = mock(Map.class);

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        () -> null,
        functions,
        columns,
        null,
        valueSerde,
        queryContext);

    // Then:
    assertThat(
        result.getExecutionStep(),
        equalTo(
            new TableAggregate(
                new ExecutionStepProperties(
                    "query.node",
                    result.getSchema(),
                    result.getKeyField()
                ),
                groupedTable.executionStep,
                functions,
                columns
            )
        )
    );
  }
}
