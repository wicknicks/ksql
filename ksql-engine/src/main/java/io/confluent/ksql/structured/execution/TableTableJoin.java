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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public class TableTableJoin implements ExecutionStep {
  private final ExecutionStepProperties properties;
  private final JoinType joinType;
  private final ExecutionStep left;
  private final ExecutionStep right;

  public TableTableJoin(
      @JsonProperty("properties") final ExecutionStepProperties properties,
      @JsonProperty("joinType") final JoinType joinType,
      @JsonProperty("left") final ExecutionStep left,
      @JsonProperty("right") final ExecutionStep right) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public Type getType() {
    return Type.JOIN;
  }

  @Override
  public List<ExecutionStep> getSources() {
    return ImmutableList.of(left, right);
  }
}
