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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StreamSource implements ExecutionStep {
  private final ExecutionStepProperties properties;
  private final String topicName;

  @JsonCreator
  public StreamSource(
      @JsonProperty("properties") final ExecutionStepProperties properties,
      @JsonProperty("topicName") final String topicName) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = topicName;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public Type getType() {
    return Type.SOURCE;
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep> getSources() {
    return Collections.emptyList();
  }
}
