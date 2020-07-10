package io.confluent.ksql.connect;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
  public void setup() throws Exception {
    dropMySqlTables();
    setupMySqlTables();
    startConnect();
    startKsql();
  }

  @After
  public void teardown() throws Exception {
    stopKsql();
    stopConnect();
    dropMySqlTables();
  }

  private void setupMySqlTables() throws Exception {
    String jdbcUrl = mysql.getJdbcUrl();
    String username = mysql.getUsername();
    String password = mysql.getPassword();
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
      conn.createStatement().executeUpdate("create table numerics (myint integer primary key auto_increment, mydecimal decimal(5,2) default 0, mynumeric numeric(7,4) default 0, myfloat float default 0, mydouble double default 0, mybit bit default 0)");
    }
  }

  private void dropMySqlTables() throws Exception {
    String jdbcUrl = mysql.getJdbcUrl();
    String username = mysql.getUsername();
    String password = mysql.getPassword();
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
      conn.createStatement().executeUpdate("drop table if exists numerics");
    }
  }

  @Test
  public void testNumerics() throws Exception {
    final String topic = "test_topic";
    final String sourced_topic = "jdbc-numerics";
    final String transformed_topic = "transformed-numerics";

    connect.kafka().createTopic(topic, 1);
    connect.kafka().createTopic(sourced_topic, 1);
    connect.kafka().createTopic(transformed_topic, 1);

    String jdbcUrl = mysql.getJdbcUrl();
    String username = mysql.getUsername();
    String password = mysql.getPassword();
    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
    conn.createStatement().executeUpdate("insert into numerics (mydecimal, mynumeric, myfloat, mydouble) values (1.87, 37.8765, 3.141517345, 2.15171819101)");

   dumpTable(conn, "numerics");

    createKsqlStream("create stream test (content varchar) with (kafka_topic='test_topic', value_format='delimited');");

    runKsqlQuery(
        "CREATE SOURCE CONNECTOR `jdbc-source` WITH(\n"
            + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSourceConnector',\n"
            + "    \"connection.url\"='" + mysql.getJdbcUrl() + "',\n"
            + "    \"mode\"='bulk',\n"
            + "    \"topic.prefix\"='jdbc-',\n"
            + "    \"table.whitelist\"='numerics',\n"
            + "    \"connection.user\"='" + mysql.getUsername() + "',\n"
            + "    \"connection.password\"='" + mysql.getPassword() + "',\n"
            + "    \"key.converter\"='JsonConverter',\n"
            + "    \"value.converter\"='JsonConverter'\n);\n"
    );

    runKsqlQuery(
        "CREATE SINK CONNECTOR `jdbc-sink` WITH(\n"
            + "    \"connector.class\"='io.confluent.connect.jdbc.JdbcSinkConnector',\n"
            + "    \"connection.url\"='" + mysql.getJdbcUrl() + "',\n"
            + "    \"topics\"='jdbc-numerics',\n"
            + "    \"connection.user\"='" + mysql.getUsername() + "',\n"
            + "    \"connection.password\"='" + mysql.getPassword() + "',\n"
            + "    \"key.converter\"='JsonConverter',\n"
            + "    \"value.converter\"='JsonConverter',\n"
            + "    \"pk.mode\"='none',\n"
            + "    \"auto.create\"='true');\n"
    );

    QueryMetadata query = runKsqlQuery(
        "SELECT UCASE(content) AS capitalized FROM test EMIT CHANGES;"
    );

    Thread.sleep(1000);

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

    TestUtils.waitForCondition(
        () -> connect.connectors().size() >= 1,
        30_000,
        "connector did not start after 30 seconds");

    assertEquals(asList("jdbc-sink", "jdbc-source"), connect.connectors());

    TestUtils.waitForCondition(
        () -> connect.connectorTopics("jdbc-source").topics().size() >= 1,
        30_000,
        "connector did not source records after 30 seconds");

    for (ConsumerRecord<byte[], byte[]> r : connect.kafka().consume(1, 30_000, sourced_topic)) {
      log.info("---- {} {}", r.key() == null ? "null" : new String(r.key()), new String(r.value()));
    }

    connect.deleteConnector("jdbc-source");

    // table name should match input topic name
    dumpTable(conn, sourced_topic);

    QueryMetadata sourceStream = runKsqlQuery(
        "CREATE STREAM jdbc_source_stream (myint INT, mydecimal DOUBLE, mynumeric DOUBLE, myfloat DOUBLE, mydouble DOUBLE, mybit INT) WITH (kafka_topic='" + sourced_topic + "', value_format='json');"
    );

    QueryMetadata transformed = runKsqlQuery(
        "CREATE STREAM transformed WITH (kafka_topic='" + transformed_topic + "', value_format='json') AS SELECT * FROM jdbc_source_stream EMIT CHANGES;"
    );

    for (ConsumerRecord<byte[], byte[]> r : connect.kafka().consume(1, 30_000, transformed_topic)) {
      log.info("---- {} {}", r.key() == null ? "null" : new String(r.key()), new String(r.value()));
    }

    transformed.close();
    sourceStream.close();
  }

  private void dumpTable(Connection conn, String tablename) throws Exception {
    ResultSet results = conn.createStatement().executeQuery("select * from `" + tablename + "`");
    assertTrue(results.next());
    ResultSetMetaData meta = results.getMetaData();
    log.info("Columns found: {}", meta.getColumnCount());
    for (int i = 0; i < meta.getColumnCount(); i++) {
      int ix = i+1;
      Object val = results.getObject(ix);
      log.info("Read column from '{}': {} value: {}, java type: {}, sql type: {}",
          meta.getTableName(ix),
          meta.getColumnName(ix),
          val,
          val == null ? "NULL" : val.getClass().getName(),
          meta.getColumnType(ix)
      );
    }
  }

}
