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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TableSink implements ExecutionStep {
  private final ExecutionStepProperties properties;
  private final ExecutionStep source;
  private final String topicName;

  public TableSink(
      @JsonProperty("properties") final ExecutionStepProperties properties,
      @JsonProperty("source") final ExecutionStep source,
      @JsonProperty("topicName") final String topicName
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public Type getType() {
    return Type.SINK;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public List<ExecutionStep> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSink tableSink = (TableSink) o;
    return Objects.equals(properties, tableSink.properties) &&
        Objects.equals(source, tableSink.source) &&
        Objects.equals(topicName, tableSink.topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, topicName);
  }
}
