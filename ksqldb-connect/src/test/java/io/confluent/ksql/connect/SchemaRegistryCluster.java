package io.confluent.ksql.connect;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.util.Properties;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegistryCluster extends io.confluent.kafka.schemaregistry.ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryCluster.class);

  protected final String kafkaTopic = "_test_schemas";

  public SchemaRegistryCluster() {
    this("localhost:9092");
  }

  public SchemaRegistryCluster(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public void start() {
    setUp();
  }

  public void setUp() {
    Properties schemaRegistryProps = getSchemaRegistryProperties();
    schemaRegistryPort = choosePort();
    schemaRegistryProps.put(
        SchemaRegistryConfig.LISTENERS_CONFIG,
        getSchemaRegistryProtocol() + "://0.0.0.0:" + schemaRegistryPort
    );
    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG, 1);
    schemaRegistryProps.put(SchemaRegistryConfig.MODE_MUTABILITY, true);
    try {
      setupRestApp(schemaRegistryProps);
    } catch (Exception e) {
      throw new RuntimeException("Could not setup rest app", e);
    }

    log.info("Starting SR at port {}", schemaRegistryPort);
  }

  @Override
  protected void setupRestApp(Properties schemaRegistryProps) throws Exception {
    restApp = new RestApp(schemaRegistryPort, null, bootstrapServers, kafkaTopic,
        compatibilityType, true, schemaRegistryProps);
    restApp.start();
  }

  public void close() {
    try {
      if (restApp != null) {
        restApp.stop();
      }
    } catch (Exception e) {
      log.warn("Exception when shutting down SR", e);
    }
  }

}
