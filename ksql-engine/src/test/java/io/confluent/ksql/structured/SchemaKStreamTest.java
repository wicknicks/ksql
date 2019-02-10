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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.execution.ExecutionStepProperties;
import io.confluent.ksql.structured.execution.ExecutionStep;
import io.confluent.ksql.structured.execution.JoinType;
import io.confluent.ksql.structured.execution.StreamFilter;
import io.confluent.ksql.structured.execution.StreamGroupBy;
import io.confluent.ksql.structured.execution.StreamSelect;
import io.confluent.ksql.structured.execution.StreamSelectKey;
import io.confluent.ksql.structured.execution.StreamSink;
import io.confluent.ksql.structured.execution.StreamSource;
import io.confluent.ksql.structured.execution.StreamStreamJoin;
import io.confluent.ksql.structured.execution.StreamTableJoin;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKStreamTest {

  private static final Expression COL0 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0");

  private static final Expression COL1 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1");

  private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private SchemaKStream initialSchemaKStream;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final Joined joined = Joined.with(
      Serdes.String(), Serdes.String(), Serdes.String(), "join");

  private KStream kStream;
  private KsqlStream<?> ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private Serde<GenericRow> leftSerde;
  private Serde<GenericRow> rightSerde;
  private Schema joinSchema;
  private Serde<GenericRow> rowSerde;
  private final Schema simpleSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("val", Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final QueryContext parentContext = queryContext.push("parent").getQueryContext();
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();

  @Mock
  private GroupedFactory mockGroupedFactory;
  @Mock
  private JoinedFactory mockJoinedFactory;
  @Mock
  private KStream mockKStream;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlStream = (KsqlStream) metaStore.getSource("TEST1");
    final StreamsBuilder builder = new StreamsBuilder();
    kStream = builder.stream(
        ksqlStream.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(),
        getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema())));

    when(mockGroupedFactory.create(anyString(), any(StringSerde.class), any(Serde.class)))
        .thenReturn(grouped);

    final KsqlStream secondKsqlStream = (KsqlStream) metaStore.getSource("ORDERS");
    final KStream secondKStream = builder
        .stream(secondKsqlStream.getKsqlTopic().getKafkaTopicName(),
            Consumed.with(Serdes.String(),
                getRowSerde(secondKsqlStream.getKsqlTopic(),
                    secondKsqlStream.getSchema())));

    final KsqlTable<?> ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final KTable kTable = builder.table(ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(),
                ksqlTable.getSchema())));

    secondSchemaKStream = buildSchemaKStreamForJoin(secondKsqlStream, secondKStream);

    leftSerde = getRowSerde(ksqlStream.getKsqlTopic(), ksqlStream.getSchema());
    rightSerde = getRowSerde(secondKsqlStream.getKsqlTopic(),
                                                     secondKsqlStream.getSchema());

    schemaKTable = new SchemaKTable<>(
        kTable,
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        parentContext,
        givenSourceStep(ksqlTable)
    );

    joinSchema = getJoinSchema(ksqlStream.getSchema(), secondKsqlStream.getSchema());

    whenCreateJoined();
  }

  private ExecutionStep givenStep(
      final String id,
      final Schema schema,
      final Field key,
      final List<ExecutionStep> sources) {
    final ExecutionStepProperties properties = mock(ExecutionStepProperties.class);
    final ExecutionStep step = mock(ExecutionStep.class);
    when(step.getProperties()).thenReturn(properties);
    when(properties.getId()).thenReturn(id);
    when(properties.getSchema()).thenReturn(schema);
    when(properties.getKey()).thenReturn(Optional.of(key));
    when(step.getSources()).thenReturn(sources);
    return step;
  }

  private ExecutionStep givenSourceStep(final Schema schema, final Field key) {
    return givenStep("source", schema, key, Collections.emptyList());
  }

  private ExecutionStep givenSourceStep(final StructuredDataSource<?> source) {
    return givenSourceStep(
        SchemaUtil.buildSchemaWithAlias(source.getSchema(), source.getName()),
        source.getKeyField().orElse(null));
  }

  private static Serde<GenericRow> getRowSerde(final KsqlTopic topic, final Schema schema) {
    return topic.getKsqlTopicSerDe().getGenericRowSerde(
        schema,
        new KsqlConfig(Collections.emptyMap()),
        false,
        MockSchemaRegistryClient::new,
        "test",
        ProcessingLogContext.create());
  }

  @Test
  public void testSelectSchemaKStream() {
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);
    Assert.assertEquals(3, projectedSchemaKStream.getSchema().fields().size());
    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL0"),
        projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL2"),
        projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL3"),
        projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL0").schema().type(),
        Schema.Type.INT64);
    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL2").schema().type(),
        Schema.Type.STRING);
    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL3").schema().type(),
        Schema.Type.FLOAT64);

    Assert.assertSame(
        projectedSchemaKStream.getExecutionStep().getSources().get(0),
        initialSchemaKStream.getExecutionStep());
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);

    // Then:
    assertThat(
        projectedSchemaKStream.getExecutionStep(),
        equalTo(
            new StreamSelect(
                new ExecutionStepProperties(
                    "query.node.child",
                    projectedSchemaKStream.getSchema(),
                    projectedSchemaKStream.getKeyField()
                ),
                initialSchemaKStream.getExecutionStep(),
                selectExpressions
            )
        )
    );
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("NEWKEY", 0, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test1;");

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(initialSchemaKStream.getKeyField()));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col0, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);
    assertThat(
        projectedSchemaKStream.getKeyField(),
        equalTo(new Field("COL0", 1, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col3 FROM test1;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        processingLogContext);
    assertThat(projectedSchemaKStream.getKeyField(), nullValue());
  }

  @Test
  public void testSelectWithExpression() {
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        processingLogContext);
    Assert.assertTrue(projectedSchemaKStream.getSchema().fields().size() == 3);
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("COL0") ==
                      projectedSchemaKStream.getSchema().fields().get(0));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_1") ==
                      projectedSchemaKStream.getSchema().fields().get(1));
    Assert.assertTrue(projectedSchemaKStream.getSchema().field("KSQL_COL_2") ==
                      projectedSchemaKStream.getSchema().fields().get(2));

    Assert.assertSame(projectedSchemaKStream.getSchema().field("COL0").schema().type(),
        Schema.Type.INT64);
    Assert.assertSame(projectedSchemaKStream.getSchema().fields().get(1).schema().type(),
        Schema.Type.INT32);
    Assert.assertSame(projectedSchemaKStream.getSchema().fields().get(2).schema().type(),
        Schema.Type.FLOAT64);

    Assert.assertSame(
        projectedSchemaKStream.getExecutionStep().getSources().get(0),
        initialSchemaKStream.getExecutionStep());
  }

  @Test
  public void testFilter() {
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        processingLogContext);

    Assert.assertTrue(filteredSchemaKStream.getSchema().fields().size() == 8);
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL0") ==
                      filteredSchemaKStream.getSchema().fields().get(2));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL1") ==
                      filteredSchemaKStream.getSchema().fields().get(3));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL2") ==
                      filteredSchemaKStream.getSchema().fields().get(4));
    Assert.assertTrue(filteredSchemaKStream.getSchema().field("TEST1.COL3") ==
                      filteredSchemaKStream.getSchema().fields().get(5));

    Assert.assertSame(filteredSchemaKStream.getSchema().field("TEST1.COL0").schema().type(),
        Schema.Type.INT64);
    Assert.assertSame(filteredSchemaKStream.getSchema().field("TEST1.COL1").schema().type(),
        Schema.Type.STRING);
    Assert.assertSame(filteredSchemaKStream.getSchema().field("TEST1.COL2").schema().type(),
        Schema.Type.STRING);
    Assert.assertSame(filteredSchemaKStream.getSchema().field("TEST1.COL3").schema().type(),
        Schema.Type.FLOAT64);

    Assert.assertSame(
        filteredSchemaKStream.getExecutionStep().getSources().get(0),
        initialSchemaKStream.getExecutionStep());
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker,
        processingLogContext);

    // Then:
    assertThat(
        filteredSchemaKStream.getExecutionStep(),
        equalTo(
            new StreamFilter(
                new ExecutionStepProperties(
                    "query.node.child",
                    initialSchemaKStream.getSchema(),
                    initialSchemaKStream.getKeyField()
                ),
                initialSchemaKStream.executionStep,
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void testSelectKey() {
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        initialSchemaKStream.getSchema().fields().get(3),
        true,
        childContextStacker);
    assertThat(rekeyedSchemaKStream.getKeyField().name().toUpperCase(), equalTo("TEST1.COL1"));
  }

  @Test
  public void shouldBuildStepForSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    // When:
    final SchemaKStream rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        initialSchemaKStream.getSchema().fields().get(3),
        true,
        childContextStacker);

    // Then;
    assertThat(
        rekeyedSchemaKStream.getExecutionStep(),
        equalTo(
            new StreamSelectKey(
                new ExecutionStepProperties(
                    "query.node.child",
                    initialSchemaKStream.getSchema(),
                    rekeyedSchemaKStream.getKeyField()
                ),
                initialSchemaKStream.executionStep,
                true
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForInto() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    // When:
    final SchemaKStream sinkKStream = initialSchemaKStream.into(
        "topic",
        rowSerde,
        Collections.emptySet(),
        childContextStacker
    );

    // Then:
    assertThat(
        sinkKStream.getExecutionStep(),
        equalTo(
            new StreamSink(
                new ExecutionStepProperties(
                    "query.node.child",
                    initialSchemaKStream.getSchema(),
                    sinkKStream.getKeyField()
                ),
                initialSchemaKStream.executionStep,
                "topic"
            )
        )
    );
  }

  @Test
  public void testGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");
    final List<Expression> groupBy = Collections.singletonList(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is("COL0"));
    assertThat(groupedSchemaKStream.getKeyField().index(), is(2));
  }

  @Test
  public void shouldBuildStepForGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");
    final List<Expression> groupBy = Collections.singletonList(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then:
    assertThat(
        groupedSchemaKStream.getExecutionStep(),
        equalTo(
            new StreamGroupBy(
                new ExecutionStepProperties(
                    "query.node.child",
                    initialSchemaKStream.getSchema(),
                    initialSchemaKStream.getKeyField()
                ),
                initialSchemaKStream.getExecutionStep(),
                groupBy
            )
        )
    );
  }

  @Test
  public void testGroupByMultipleColumns() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");

    final List<Expression> groupBy = ImmutableList.of(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1"),
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is("TEST1.COL1|+|TEST1.COL0"));
    assertThat(groupedSchemaKStream.getKeyField().index(), is(-1));
  }

  @Test
  public void shouldBuildStepForGroupByChangeKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");
    final List<Expression> groupBy = ImmutableList.of(
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL1"),
        new DereferenceExpression(
            new QualifiedNameReference(QualifiedName.of("TEST1")), "COL0")
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        groupBy,
        childContextStacker);

    // Then;
    assertThat(
        groupedSchemaKStream.getExecutionStep(),
        equalTo(
            new StreamGroupBy(
                new ExecutionStepProperties(
                    "query.node.child",
                    initialSchemaKStream.getSchema(),
                    groupedSchemaKStream.getKeyField()
                ),
                initialSchemaKStream.getExecutionStep(),
                groupBy
            )
        )
    );
  }

  @Test
  public void testGroupByMoreComplexExpression() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100;");

    final Expression groupBy = new FunctionCall(QualifiedName.of("UCASE"), ImmutableList.of(COL1));

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        rowSerde,
        ImmutableList.of(groupBy),
        childContextStacker);

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().name(), is("UCASE(TEST1.COL1)"));
    assertThat(groupedSchemaKStream.getKeyField().index(), is(-1));
  }

  @Test
  public void shouldUseFactoryForGroupedWithoutRekey() {
    // Given:
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    final Expression keyExpression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())),
        ksqlStream.getKeyField().get().name());
    final List<Expression> groupByExpressions = Collections.singletonList(keyExpression);
    initialSchemaKStream
        = buildSchemaKStream(mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(
        leftSerde,
        groupByExpressions,
        childContextStacker);

    // Then:
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        any(StringSerde.class),
        same(leftSerde)
    );
    verify(mockKStream).groupByKey(same(grouped));
  }

  @Test
  public void shouldUseFactoryForGrouped() {
    // Given:
    when(mockKStream.filter(any(Predicate.class))).thenReturn(mockKStream);
    final KGroupedStream groupedStream = mock(KGroupedStream.class);
    when(mockKStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);
    final Expression col0Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL0");
    final Expression col1Expression = new DereferenceExpression(
        new QualifiedNameReference(QualifiedName.of(ksqlStream.getName())), "COL1");
    final List<Expression> groupByExpressions = Arrays.asList(col1Expression, col0Expression);
    initialSchemaKStream = buildSchemaKStream(mockKStream, mockGroupedFactory, mockJoinedFactory);

    // When:
    initialSchemaKStream.groupBy(
        leftSerde,
        groupByExpressions,
        childContextStacker);

    // Then:
    verify(mockGroupedFactory).create(
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
        any(StringSerde.class),
        same(leftSerde));
    verify(mockKStream).groupBy(any(KeyValueMapper.class), same(grouped));
  }

  private void givenStreamStreamLeftJoin() {
    when(
        mockKStream.leftJoin(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
  }

  private SchemaKStream whenStreamStreamLeftJoin(final JoinWindows joinWindow) {
    return initialSchemaKStream.leftJoin(secondSchemaKStream,
        joinSchema,
        joinSchema.fields().get(0),
        joinWindow,
        leftSerde,
        rightSerde,
        childContextStacker);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamLeftJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamLeftJoin(joinWindow);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).leftJoin(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(ExecutionStep.Type.JOIN, joinedKStream.getExecutionStep().getType());
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertEquals(joinSchema.fields().get(0), joinedKStream.getKeyField());
    assertEquals(
        Arrays.asList(
            initialSchemaKStream.getExecutionStep(),
            secondSchemaKStream.getExecutionStep()),
        joinedKStream.getExecutionStep().getSources());
  }

  @Test
  public void shouldBuildStepForStreamStreamLeftJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamLeftJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamLeftJoin(joinWindow);

    // Then:
    final ExecutionStep step = joinedKStream.getExecutionStep();
    assertThat(
        step,
        equalTo(
            new StreamStreamJoin(
                new ExecutionStepProperties(
                    "query.node.child",
                    joinSchema,
                    joinSchema.fields().get(0)
                ),
                JoinType.LEFT,
                initialSchemaKStream.executionStep,
                secondSchemaKStream.executionStep
            )
        )
    );
  }

  private void givenStreamStreamJoin() {
    when(
        mockKStream.join(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
  }

  private SchemaKStream whenStreamStreamJoin(final JoinWindows joinWindow) {
    return initialSchemaKStream.join(secondSchemaKStream,
        joinSchema,
        joinSchema.fields().get(0),
        joinWindow,
        leftSerde,
        rightSerde,
        childContextStacker);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamJoin(joinWindow);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).join(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(ExecutionStep.Type.JOIN, joinedKStream.getExecutionStep().getType());
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertEquals(joinSchema.fields().get(0), joinedKStream.getKeyField());
    assertEquals(
        Arrays.asList(
            initialSchemaKStream.getExecutionStep(),
            secondSchemaKStream.getExecutionStep()
        ),
        joinedKStream.getExecutionStep().getSources());
  }

  @Test
  public void shouldBuildStepForStreamStreamJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamJoin(joinWindow);

    // Then:
    final ExecutionStep step = joinedKStream.getExecutionStep();
    assertThat(
        step,
        equalTo(
            new StreamStreamJoin(
                new ExecutionStepProperties(
                    "query.node.child",
                    joinSchema,
                    joinSchema.fields().get(0)
                ),
                JoinType.INNER,
                initialSchemaKStream.executionStep,
                secondSchemaKStream.executionStep
            )
        )
    );
  }

  private void givenStreamStreamOuterJoin() {
    when(
        mockKStream.outerJoin(
            any(KStream.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(JoinWindows.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
  }

  private SchemaKStream whenStreamStreamOuterJoin(final JoinWindows joinWindow) {
    return initialSchemaKStream.outerJoin(secondSchemaKStream,
        joinSchema,
        joinSchema.fields().get(0),
        joinWindow,
        leftSerde,
        rightSerde,
        childContextStacker
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamOuterJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamOuterJoin(joinWindow);

    // Then:
    verifyCreateJoined(rightSerde);
    verify(mockKStream).outerJoin(
        eq(secondSchemaKStream.kstream),
        any(SchemaKStream.KsqlValueJoiner.class),
        eq(joinWindow),
        same(joined)
    );
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(ExecutionStep.Type.JOIN, joinedKStream.getExecutionStep().getType());
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertEquals(joinSchema.fields().get(0), joinedKStream.getKeyField());
    assertEquals(
        Arrays.asList(
            initialSchemaKStream.getExecutionStep(),
            secondSchemaKStream.executionStep
        ),
        joinedKStream.getExecutionStep().getSources()
    );
  }

  @Test
  public void shouldBuildStepForStreamStreamOuterJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));
    givenStreamStreamOuterJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamStreamOuterJoin(joinWindow);

    // Then:
    final ExecutionStep step = joinedKStream.getExecutionStep();
    assertThat(
        step,
        equalTo(
            new StreamStreamJoin(
                new ExecutionStepProperties(
                    "query.node.child",
                    joinSchema,
                    joinSchema.fields().get(0)
                ),
                JoinType.OUTER,
                initialSchemaKStream.executionStep,
                secondSchemaKStream.executionStep
            )
        )
    );
  }

  private void givenStreamTableLeftJoin() {
    when(
        mockKStream.leftJoin(
            any(KTable.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
  }

  private SchemaKStream whenStreamTableLeftJoin() {
    return initialSchemaKStream.leftJoin(
        schemaKTable,
        joinSchema,
        joinSchema.fields().get(0),
        leftSerde,
        childContextStacker);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    givenStreamTableLeftJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamTableLeftJoin();

    // Then:
    verifyCreateJoined(null);
    verify(mockKStream).leftJoin(
        eq(schemaKTable.getKtable()),
        any(SchemaKStream.KsqlValueJoiner.class),
        same(joined));
    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(ExecutionStep.Type.JOIN, joinedKStream.getExecutionStep().getType());
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertEquals(joinSchema.fields().get(0), joinedKStream.getKeyField());
    assertEquals(
        Arrays.asList(
            initialSchemaKStream.getExecutionStep(),
            schemaKTable.getExecutionStep()
        ),
        joinedKStream.getExecutionStep().getSources()
    );
  }

  @Test
  public void shouldBuildStepForStreamToTableLeftJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    givenStreamTableLeftJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamTableLeftJoin();

    // Then:
    final ExecutionStep step = joinedKStream.getExecutionStep();
    assertThat(
        step,
        equalTo(
            new StreamTableJoin(
                new ExecutionStepProperties(
                    "query.node.child",
                    joinSchema,
                    joinSchema.fields().get(0)
                ),
                JoinType.LEFT,
                initialSchemaKStream.executionStep,
                schemaKTable.executionStep
            )
        )
    );
  }

  private void givenStreamTableJoin() {
    when(
        mockKStream.join(
            any(KTable.class),
            any(SchemaKStream.KsqlValueJoiner.class),
            any(Joined.class))
    ).thenReturn(mockKStream);
  }

  private SchemaKStream whenStreamTableJoin() {
    return initialSchemaKStream.join(
        schemaKTable,
        joinSchema,
        joinSchema.fields().get(0),
        leftSerde,
        childContextStacker
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    givenStreamTableJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamTableJoin();

    // Then:
    verifyCreateJoined(null);
    verify(mockKStream).join(
        eq(schemaKTable.getKtable()),
        any(SchemaKStream.KsqlValueJoiner.class),
        same(joined)
    );

    assertThat(joinedKStream, instanceOf(SchemaKStream.class));
    assertEquals(ExecutionStep.Type.JOIN, joinedKStream.getExecutionStep().getType());
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertEquals(joinSchema.fields().get(0), joinedKStream.getKeyField());
    assertEquals(
        Arrays.asList(
            initialSchemaKStream.getExecutionStep(),
            schemaKTable.getExecutionStep()
        ),
        joinedKStream.getExecutionStep().getSources()
    );
  }

  @Test
  public void shouldBuildStepForStreamTableInnerJoin() {
    // Given:
    buildSchemaKStreamForJoin();
    givenStreamTableJoin();

    // When:
    final SchemaKStream joinedKStream = whenStreamTableJoin();

    // Then:
    final ExecutionStep step = joinedKStream.getExecutionStep();
    assertThat(
        step,
        equalTo(
            new StreamTableJoin(
                new ExecutionStepProperties(
                "query.node.child",
                    joinSchema,
                    joinSchema.fields().get(0)
                ),
                JoinType.INNER,
                initialSchemaKStream.executionStep,
                schemaKTable.executionStep
            )
        )
    );
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectly() {
    // Given:
    final SchemaKStream schemaKtream = new SchemaKStream(
        mock(KStream.class),
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext(),
        givenSourceStep(simpleSchema, simpleSchema.field("key")));

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key : VARCHAR, val : BIGINT] | Logger: source\n";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyWhenParents() {
    // Given:
    final Schema source1Schema = SchemaBuilder.struct()
        .field("src1f", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    final ExecutionStep source1 = givenStep(
        "src1",
        source1Schema,
        source1Schema.fields().get(0),
        Collections.emptyList());
    final Schema source2Schema = SchemaBuilder.struct()
        .field("src2f", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    final ExecutionStep source2 = givenStep(
        "src1",
        source2Schema,
        source1Schema.fields().get(0),
        Collections.emptyList());
    final SchemaKStream schemaKtream = new SchemaKStream(
        mock(KStream.class),
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext(),
        givenStep(
            "step",
            simpleSchema,
            simpleSchema.field("key"),
            ImmutableList.of(source1, source2))
    );

    // When/Then:
    final String expected =
        " > [ SOURCE ] | Schema: [key : VARCHAR, val : BIGINT] | Logger: step\n\t\t "
            + "> [ SOURCE ] | Schema: [src1f : VARCHAR] | Logger: src1\n\t\t "
            + "> [ SOURCE ] | Schema: [src2f : VARCHAR] | Logger: src1\n";
    assertThat(schemaKtream.getExecutionPlan(""), equalTo(expected));
  }

  private void whenCreateJoined() {
    when(
        mockJoinedFactory.create(
            any(StringSerde.class),
            any(Serde.class),
            any(),
            anyString())
    ).thenReturn(joined);
  }

  private void verifyCreateJoined(final Serde<GenericRow> rightSerde) {
    verify(mockJoinedFactory).create(
        any(StringSerde.class),
        same(leftSerde),
        same(rightSerde),
        eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext()))
    );
  }

  private SchemaKStream buildSchemaKStream(
      final KStream kStream,
      final StreamsFactories streamsFactories,
      final ExecutionStep executionStep) {
    return new SchemaKStream(
        kStream,
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        streamsFactories,
        parentContext,
        executionStep
    );
  }

  private SchemaKStream buildSchemaKStream(
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return buildSchemaKStream(
        kStream,
        new StreamsFactories(groupedFactory, joinedFactory, mock(MaterializedFactory.class)),
        givenSourceStep(ksqlStream)
    );
  }

  // todo: clean these bottom 2 up

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final KStream kStream) {
    return buildSchemaKStreamForJoin(
        ksqlStream,
        kStream,
        GroupedFactory.create(ksqlConfig),
        JoinedFactory.create(ksqlConfig));
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KsqlStream ksqlStream,
      final KStream kStream,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory) {
    return buildSchemaKStream(
        kStream,
        new StreamsFactories(groupedFactory, joinedFactory, mock(MaterializedFactory.class)),
        givenSourceStep(ksqlStream));
  }

  private void buildSchemaKStreamForJoin() {
    initialSchemaKStream = buildSchemaKStream(
        mockKStream,
        new StreamsFactories(
            mockGroupedFactory,
            mockJoinedFactory,
            mock(MaterializedFactory.class)
        ),
        givenSourceStep(ksqlStream)
    );
  }

  private static Schema getJoinSchema(final Schema leftSchema, final Schema rightSchema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Field field : leftSchema.fields()) {
      final String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (final Field field : rightSchema.fields()) {
      final String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(selectQuery, metaStore);

    initialSchemaKStream = new SchemaKStream(
        kStream,
        Serdes::String,
        ksqlConfig,
        functionRegistry,
        queryContext.push("source").getQueryContext(),
        new StreamSource(
            new ExecutionStepProperties(
                "source",
                logicalPlan.getTheSourceNode().getSchema(),
                ksqlStream.getKeyField().orElse(null)
            ),
            ksqlStream.getKafkaTopicName()
        )
    );

    rowSerde = new KsqlJsonTopicSerDe().getGenericRowSerde(
        initialSchemaKStream.getSchema(),
        null,
        false,
        () -> null,
        "test",
        processingLogContext);

    return logicalPlan;
  }
}
