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

package io.confluent.ksql.execution.plan;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableGroupByTest {
  @Mock
  private ExecutionStepPropertiesV1 properties1;
  @Mock
  private ExecutionStepPropertiesV1 properties2;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> source1;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> source2;
  @Mock
  private Formats formats1;
  @Mock
  private Formats formats2;
  @Mock
  private ColumnName alias;

  private List<Expression> expression1;
  private List<Expression> expression2;

  @Before
  public void setup() {
    expression1 = ImmutableList.of(mock(Expression.class));
    expression2 = ImmutableList.of(mock(Expression.class));
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableGroupBy<>(properties1, source1, formats1, expression1, Optional.of(alias)),
            new TableGroupBy<>(properties1, source1, formats1, expression1, Optional.of(alias)))
        .addEqualityGroup(new TableGroupBy<>(properties2, source1, formats1, expression1, Optional.of(alias)))
        .addEqualityGroup(new TableGroupBy<>(properties1, source2, formats1, expression1, Optional.of(alias)))
        .addEqualityGroup(new TableGroupBy<>(properties1, source1, formats2, expression1, Optional.of(alias)))
        .addEqualityGroup(new TableGroupBy<>(properties1, source1, formats1, expression2, Optional.of(alias)))
        .addEqualityGroup(new TableGroupBy<>(properties1, source1, formats1, expression1, Optional.empty()));
  }
}