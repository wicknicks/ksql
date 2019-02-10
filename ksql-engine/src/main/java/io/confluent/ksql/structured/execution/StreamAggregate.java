/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StreamAggregate implements ExecutionStep {
  private final ExecutionStepProperties properties;
  private final ExecutionStep source;
  private final Map<Integer, AggregateFieldMapping> fieldMapping;

  @JsonCreator
  public StreamAggregate(
      @JsonProperty("properties") final ExecutionStepProperties properties,
      @JsonProperty("source") final ExecutionStep source,
      @JsonProperty("fieldMapping") final Map<Integer, AggregateFieldMapping> fieldMapping) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.fieldMapping = Objects.requireNonNull(fieldMapping, "fieldMapping");
  }

  public StreamAggregate(
      final ExecutionStepProperties properties,
      final ExecutionStep source,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    fieldMapping = AggregateFieldMapping.buildMappings(aggValToFunctionMap, aggValToValColumnMap);
  }

  @Override
  public Type getType() {
    return Type.AGGREGATE;
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep> getSources() {
    return Collections.singletonList(source);
  }
}
