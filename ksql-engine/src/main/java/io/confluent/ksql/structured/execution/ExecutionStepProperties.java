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
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class ExecutionStepProperties {
  private final String id;
  private final Schema schema;
  private final Optional<Field> key;

  @JsonCreator
  public ExecutionStepProperties(
      @JsonProperty("id") final String id,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("key") final Field key) {
    this.id = Objects.requireNonNull(id, "id");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.key = Optional.ofNullable(key);
  }

  public Schema getSchema() {
    return schema;
  }

  public Optional<Field> getKey() {
    return key;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecutionStepProperties that = (ExecutionStepProperties) o;
    return Objects.equals(id, that.id)
        && Objects.equals(schema, that.schema)
        && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, schema, key);
  }

  @Override
  public String toString() {
    return "ExecutionStepProperties{"
        + "id='" + id + '\''
        + ", schema=" + schema
        + ", key=" + key
        + '}';
  }
}
