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
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AggregateFieldMapping {
  final Optional<String> function;
  final int argIndex;

  public static Map<Integer, AggregateFieldMapping> buildMappings(
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap) {
    Objects.requireNonNull(aggValToFunctionMap, "aggValToValFunctionMap");
    Objects.requireNonNull(aggValToValColumnMap, "aggValToValColumnMap");
    final Map<Integer, AggregateFieldMapping> fieldMapping = new HashMap<>();
    aggValToFunctionMap.forEach(
        (i, f) ->
            fieldMapping.put(
                i,
                new AggregateFieldMapping(f.getFunctionName(), f.getArgIndexInValue()))
    );
    aggValToValColumnMap.forEach(
        (i, j) -> fieldMapping.put(i, new AggregateFieldMapping(null, j)));
    return ImmutableMap.copyOf(fieldMapping);
  }

  @JsonCreator
  public AggregateFieldMapping(
      @JsonProperty("function") final String function,
      @JsonProperty("argIndex") final int argIndex
  ) {
    this.function = Optional.ofNullable(function);
    this.argIndex = Objects.requireNonNull(argIndex, "argIndex");
  }

  public int getArgIndex() {
    return argIndex;
  }

  public Optional<String> getFunction() {
    return function;
  }
}
