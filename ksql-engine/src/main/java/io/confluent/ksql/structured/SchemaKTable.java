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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.execution.ExecutionStep;
import io.confluent.ksql.structured.execution.ExecutionStepProperties;
import io.confluent.ksql.structured.execution.JoinType;
import io.confluent.ksql.structured.execution.TableFilter;
import io.confluent.ksql.structured.execution.TableGroupBy;
import io.confluent.ksql.structured.execution.TableOverwriteSchema;
import io.confluent.ksql.structured.execution.TableSelect;
import io.confluent.ksql.structured.execution.TableSink;
import io.confluent.ksql.structured.execution.TableTableJoin;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable<K> extends SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final KTable<K, GenericRow> ktable;

  public SchemaKTable(
      final KTable<K, GenericRow> ktable,
      final SerdeFactory<K> keySerdeFactory,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext queryContext,
      final ExecutionStep executionStep
  ) {
    this(
        ktable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        StreamsFactories.create(ksqlConfig),
        queryContext,
        executionStep
    );
  }

  SchemaKTable(
      final KTable<K, GenericRow> ktable,
      final SerdeFactory<K> keySerdeFactory,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final StreamsFactories streamsFactories,
      final QueryContext queryContext,
      final ExecutionStep executionStep
  ) {
    super(
        null,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        streamsFactories,
        queryContext,
        executionStep
    );
    this.ktable = ktable;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<K> into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes,
      final QueryContext.Stacker contextStacker
  ) {

    ktable.toStream()
        .mapValues(row -> {
              if (row == null) {
                return null;
              }
              final List<Object> columns = new ArrayList<>();
              for (int i = 0; i < row.getColumns().size(); i++) {
                if (!rowkeyIndexes.contains(i)) {
                  columns.add(row.getColumns().get(i));
                }
              }
              return new GenericRow(columns);
            }
        ).to(kafkaTopicName, Produced.with(keySerdeFactory.create(), topicValueSerDe));

    return new SchemaKTable<>(
        ktable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        queryContext,
        new TableSink(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                getSchema(),
                getKeyField()
            ),
            executionStep,
            kafkaTopicName
        )
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        getSchema(),
        hasWindowedKey(),
        ksqlConfig,
        functionRegistry,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(ExecutionStep.Type.FILTER.name()).getQueryContext()))
    );
    final KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable<>(
        filteredKTable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableFilter(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                getSchema(),
                getKeyField()
            ),
            executionStep,
            filterExpression
        )
    );
  }

  @Override
  public SchemaKTable<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    final Selection selection = new Selection(
        selectExpressions,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(ExecutionStep.Type.PROJECT.name()).getQueryContext()))
    );
    return new SchemaKTable<>(
        ktable.mapValues(selection.getSelectValueMapper()),
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableSelect(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                selection.getProjectedSchema(),
                selection.getKey()
            ),
            executionStep,
            selectExpressions
        )
    );
  }

  @SuppressWarnings("unchecked") // needs investigating
  @Override
  public KStream getKstream() {
    return ktable.toStream();
  }

  public KTable getKtable() {
    return ktable;
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker) {

    final GroupBy groupBy = new GroupBy(groupByExpressions);

    final KGroupedTable kgroupedTable = ktable
        .filter((key, value) -> value != null)
        .groupBy(
            (key, value) -> new KeyValue<>(groupBy.mapper.apply(key, value), value),
            streamsFactories.getGroupedFactory().create(
                StreamsUtil.buildOpName(
                    contextStacker.getQueryContext()), Serdes.String(), valSerde)
        );

    final Field newKeyField = new Field(
        groupBy.aggregateKeyName, -1, Schema.OPTIONAL_STRING_SCHEMA);
    return new SchemaKGroupedTable(
        kgroupedTable,
        ksqlConfig,
        functionRegistry,
        new TableGroupBy(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                getSchema(),
                newKeyField
            ),
            executionStep,
            groupByExpressions
        )
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable = ktable.join(
        schemaKTable.getKtable(),
        new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
    );

    return new SchemaKTable<>(
        joinedKTable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableTableJoin(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                joinSchema,
                joinKey
            ),
            JoinType.INNER,
            executionStep,
            schemaKTable.executionStep
        )
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable =
        ktable.leftJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable<>(
        joinedKTable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableTableJoin(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                joinSchema,
                joinKey
            ),
            JoinType.LEFT,
            executionStep,
            schemaKTable.executionStep
        )
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable =
        ktable.outerJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable<>(
        joinedKTable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableTableJoin(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                joinSchema,
                joinKey
            ),
            JoinType.OUTER,
            executionStep,
            schemaKTable.executionStep
        )
    );
  }

  public SchemaKTable<K> overwriteSchema(
      final Schema schema,
      final QueryContext.Stacker contextStacker
  ) {
    return new SchemaKTable<>(
        ktable,
        keySerdeFactory,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext(),
        new TableOverwriteSchema(
            new ExecutionStepProperties(
                QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext()),
                schema,
                getKeyField()
            ),
            executionStep
        )
    );
  }
}
