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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StreamOverwriteSchemaAndKey implements ExecutionStep {
  private final ExecutionStepProperties properties;
  private final ExecutionStep source;

  public StreamOverwriteSchemaAndKey(
      final ExecutionStepProperties properties,
      final ExecutionStep source) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
  }

  @Override
  public List<ExecutionStep> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public Type getType() {
    return Type.OVERWRITE_SCHEMA;
  }
}
