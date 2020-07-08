package io.confluent.ksql.connect;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

@Category(IntegrationTest.class)
public class MySqlIT extends BaseIT {

  private static final Logger log = LoggerFactory.getLogger(MySqlIT.class);

  @Rule
  public MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.20");

  @Before
  public void setup() {
    startConnect();
    startKsql();
  }

  @After
  public void teardown() {
    stopKsql();
    stopConnect();
  }

  @Test
  public void foo() throws Exception {
    final String topic = "test_topic";
    connect.kafka().createTopic(topic, 1);

    String jdbcUrl = mysql.getJdbcUrl();
    String username = mysql.getUsername();
    String password = mysql.getPassword();
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    ResultSet resultSet = conn.createStatement().executeQuery("SELECT 1");
    resultSet.next();
    int result = resultSet.getInt(1);

    assertEquals(1, result);

    createKsqlStream("create stream test (content varchar) with (kafka_topic='test_topic', value_format='delimited');");

    QueryMetadata query = runKsqlQuery(
        "SELECT UCASE(content) AS capitalized FROM test EMIT CHANGES;"
    );

    Thread.sleep(100);

    connect.kafka().produce(topic, "hello");
    connect.kafka().produce(topic, "world");
    connect.kafka().produce(topic, "one");
    connect.kafka().produce(topic, "two");
    connect.kafka().produce(topic, "three");
    connect.kafka().produce(topic, "four"); // this shouldn't be returned in the query

    final BlockingRowQueue rowQueue = ((TransientQueryMetadata) query).getRowQueue();
    final int expectedRows = 5;

    log.info("Size {} ", rowQueue.size());

    TestUtils.waitForCondition(
        () -> rowQueue.size() >= expectedRows,
        30_000,
        expectedRows + " rows were not available after 30 seconds");

    final List<GenericRow> rows = new ArrayList<>();
    rowQueue.drainTo(rows);

    rows.forEach(r -> log.info(">>> {}", r));

    query.close();
  }

}
