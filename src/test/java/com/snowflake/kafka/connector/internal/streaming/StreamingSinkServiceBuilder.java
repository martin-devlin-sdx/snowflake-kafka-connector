package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkTask;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.TopicMapping;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;

import java.util.*;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class StreamingSinkServiceBuilder {

  private final SnowflakeConnectionService conn;
  private final Map<String, String> connectorConfig;

  private KafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
  private SinkTaskContext sinkTaskContext = new InMemorySinkTaskContext(Collections.emptySet());
  private boolean enableCustomJMXMonitoring = false;
  private TopicMapping topicMapping;
  private Map<String, String> prefixTopicToSchemaMap;
  private SchemaEvolutionService schemaEvolutionService;

  public static StreamingSinkServiceBuilder builder(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    return new StreamingSinkServiceBuilder(conn, connectorConfig);
  }

  public SnowflakeSinkServiceV2 build() {
    return new SnowflakeSinkServiceV2(
        conn,
        connectorConfig,
        errorReporter,
        sinkTaskContext,
        enableCustomJMXMonitoring,
        topicMapping,
        prefixTopicToSchemaMap,
        schemaEvolutionService == null
            ? new SnowflakeSchemaEvolutionService(conn)
            : schemaEvolutionService);
  }

  private StreamingSinkServiceBuilder(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    this.conn = conn;
    this.connectorConfig = connectorConfig;
  }

  public StreamingSinkServiceBuilder withErrorReporter(
      InMemoryKafkaRecordErrorReporter errorReporter) {
    this.errorReporter = errorReporter;
    return this;
  }

  public StreamingSinkServiceBuilder withSinkTaskContext(SinkTaskContext sinkTaskContext) {
    this.sinkTaskContext = sinkTaskContext;
    return this;
  }

  public StreamingSinkServiceBuilder withEnableCustomJMXMetrics(boolean enableCustomJMXMetrics) {
    this.enableCustomJMXMonitoring = enableCustomJMXMetrics;
    return this;
  }

  public StreamingSinkServiceBuilder withTopicToTableMap(Map<String, String> topic2TableMap) {
    this.topicMapping = new MockTopicMapping(topic2TableMap);
    return this;
  }

  public StreamingSinkServiceBuilder withPrefixTopicToTableMap(Map<String,String> prefixTopicToSchemaMap) {
    this.prefixTopicToSchemaMap = prefixTopicToSchemaMap;
    return this;
  }

  public StreamingSinkServiceBuilder withSchemaEvolutionService(
      SchemaEvolutionService schemaEvolutionService) {
    this.schemaEvolutionService = schemaEvolutionService;
    return this;
  }

  public static class MockTopicMapping implements TopicMapping{
      private final Map<String, String> topic2TableMap;
      MockTopicMapping(Map<String, String> topic2TableMap){
          this.topic2TableMap = topic2TableMap;
      }
    @Override
    public boolean validate(Map<String, String> connectorConfig, Config config) {
      return true;
    }
    @Override
    public void start(Map<String, String> connectorConfig, SnowflakeSinkTask task) {
    }
    @Override
    public Set<String> getAllSchemas() {
      return Collections.singleton("");
    }
    @Override
    public String getSchema(String topic) {
      return "";
    }
    @Override
    public String getTable(String topic) {
      return  Utils.tableName(topic, topic2TableMap);
    }
    @Override
    public Map<String, String> getTopic2table() {
      return topic2TableMap;
    }
  }
}
