package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.*;

import java.util.HashMap;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.*;
import static com.snowflake.kafka.connector.Utils.SF_DATABASE;
import static com.snowflake.kafka.connector.config.MultiSchemaTopicMappingTest.assertError;
import static com.snowflake.kafka.connector.config.MultiSchemaTopicMappingTest.assertNoErrors;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultTopicMappingTest {

    private HashMap<String, String> connectorConfig;
    private DefaultTopicMapping tm;
    private Config config;

    @Before
    public void setupTopicMapping(){
        // use the 'getTopicMappingProvdier' factory method to create the MultiSchemaTopicMapping
        connectorConfig = new HashMap<>(); // when no TOPIC_MAPPING_PROVIDER_CLASS is defined it should default to DefaultTopicMapping
        TopicMapping topicMapping = SnowflakeSinkConnector.getTopicMappingProvider(connectorConfig);
        assertThat(topicMapping).isInstanceOf(DefaultTopicMapping.class);
        tm = (DefaultTopicMapping) topicMapping;

        connectorConfig.put(TOPICS_TABLES_MAP, "sdx.sei.hcd_test_stg.test_schemaA.kafka_events:KAFKA_EVENTS,sdx.sei.hcd_test_stg.test_schemaB.kafka_events:KAFKA_EVENTS2");
        connectorConfig.put(SF_URL, "url");
        connectorConfig.put(SF_USER, "user");
        connectorConfig.put(SF_DATABASE, "database");
        connectorConfig.put(INGESTION_METHOD_OPT, SNOWPIPE_STREAMING.toString());
        connectorConfig.put(SF_SCHEMA, "TestSchema");
        updateConfig();
    }

    private void updateConfig() {
        // this is what SnowflakeSinkConnector does to create the Config
        ConfigDef configDef = ConnectorConfigDefinition.getConfig();
        config = new Config(configDef.validate(connectorConfig));
    }

    @Test
    public void testDefaultSchemaMapping() {
        tm.validate(connectorConfig, config);
        tm.start(connectorConfig);
        assertNoErrors(config);

        // always use the same schema.
        assertThat( tm.getSchema("anytopic") ).isEqualTo("TestSchema");
    }

    @Test
    public void testInvalidConfig_SchemaMissing(){
        connectorConfig.remove(SF_SCHEMA);
        updateConfig();
        tm.validate(connectorConfig, config);
        assertError(SF_SCHEMA, "snowflake.schema.name must be provided", config);
    }

    @Test
    public void testInvalidConfig_usesMultiSchemaMapping(){
        connectorConfig.put(TOPIC_PREFIX_TO_SCHEMA_MAP, "sdx.sei.hcd_test_stg.test_schemaA:TEST_SCHEMA,sdx.sei.hcd_test_stg.test_schemaB:TEST_SCHEMA2");
        updateConfig();
        tm.validate(connectorConfig, config);
        assertError(TOPIC_PREFIX_TO_SCHEMA_MAP, "snowflake.topicPrefix2schema.map must not be used for for single. Use only snowflake.schema.name", config);
    }

    @Test
    public void testGetAllSchemas(){
        tm.start(connectorConfig);
        assertThat(tm.getAllSchemas()).containsOnly("TestSchema");
    }
}
