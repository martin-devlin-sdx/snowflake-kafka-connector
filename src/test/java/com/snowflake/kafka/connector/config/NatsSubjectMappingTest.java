package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.NatsListenerServiceTest.MockSnowflakeSinkTask;
import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.Utils;
import io.nats.client.support.NatsUri;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.*;
import static com.snowflake.kafka.connector.config.MultiSchemaTopicMappingTest.getErrors;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;
import static org.assertj.core.api.Assertions.assertThat;

public class NatsSubjectMappingTest {

    public Map<String,String> connectorConfig;
    Config config;
    NatsSubjectMapping tm;

    @Before
    public void setupTopicMapping(){
        // use the 'getTopicMappingProvdier' factory method to create the MultiSchemaTopicMapping
        connectorConfig = new HashMap<>();
        connectorConfig.put(TOPIC_MAPPING_PROVIDER_CLASS, NatsSubjectMapping.class.getName());
        TopicMapping topicMapping = SnowflakeSinkConnector.getTopicMappingProvider(connectorConfig);
        assertThat(topicMapping).isInstanceOf(NatsSubjectMapping.class);
        tm = (NatsSubjectMapping) topicMapping;

        connectorConfig.put(NATS_TABLES_MAP, "app.prebill.>:NATS_PREBILL_EVENTS,app.pieces.>:NATS_PIECES_EVENTS");
        connectorConfig.put(NATS_SUBJECT_PREFIX_TO_SCHEMA_MAP, "app.prebill:PREBILL_SCHEMA,app.pieces:PIECES_SCHEMA");
        connectorConfig.put(SF_URL, "url");
        connectorConfig.put(SF_USER, "user");
        connectorConfig.put(SF_DATABASE, "database");
        connectorConfig.put(INGESTION_METHOD_OPT, SNOWPIPE_STREAMING.toString());

        connectorConfig.put(Utils.ACTUAL_MAX_TASKS, "4"); // 4 threads
        connectorConfig.put(Utils.NATS_URL, "nats://127.0.0.1:4222");

        updateConfig();
    }

    private void updateConfig() {
        // this is what SnowflakeSinkConnector does to create the Config
        ConfigDef configDef = ConnectorConfigDefinition.getConfig();
        config = new Config(configDef.validate(connectorConfig));
    }

    @Test
    public void testValidate() {
        boolean valid = tm.validate(connectorConfig, config);
        assertThat(valid).isTrue();
        assertThat(getErrors(config)).isEmpty();
        List<NatsUri> natsServerUris = NatsSubjectMapping.getConnectionOptions(connectorConfig).getNatsServerUris();
        assertThat(natsServerUris).hasSize(1);
        assertThat(natsServerUris.get(0).toString()).isEqualTo("nats://127.0.0.1:4222");
    }

    @Test
    public void testGetSubscriptions(){
        // ensure the NATS subscriptions still contain the > symbol if it is defined
        boolean valid = tm.validate(connectorConfig, config);
        assertThat(valid).isTrue();
        assertThat(getErrors(config)).isEmpty();

        List<String> subscriptions = tm.getSubscriptions(connectorConfig);
        assertThat(subscriptions).containsOnly("app.prebill.>", "app.pieces.>");

        // ensure that the topic to table mapping has converted the .> to a regex .*
        MockSnowflakeSinkTask sinkTask = new MockSnowflakeSinkTask();
        tm.start(connectorConfig, sinkTask);
        assertThat(tm.getTopic2table()).containsKeys("app.prebill.*", "app.pieces.*");

        // tidy up
        tm.stop(sinkTask);
    }

    @Test
    public void testGetTable(){
        tm.validate(connectorConfig, config);
        MockSnowflakeSinkTask sinkTask = new MockSnowflakeSinkTask();
        tm.start(connectorConfig, sinkTask);
        // test that the NATS > wildcard works as expected
        assertThat(tm.getTable("app.prebill.test")).isEqualTo("NATS_PREBILL_EVENTS");
        assertThat(tm.getTable("app.prebill.test.one")).isEqualTo("NATS_PREBILL_EVENTS");
        assertThat(tm.getTable("app.prebill.test.one.two")).isEqualTo("NATS_PREBILL_EVENTS");

        assertThat(tm.getTable("app.pieces.test")).isEqualTo("NATS_PIECES_EVENTS");
        assertThat(tm.getTable("app.pieces.test.one")).isEqualTo("NATS_PIECES_EVENTS");
        assertThat(tm.getTable("app.pieces.test.one.two")).isEqualTo("NATS_PIECES_EVENTS");

        // tidy up
        tm.stop(sinkTask);

        // if there is no mapping it should still generate a new table based on the topic name
        // - the generated name will have a hashcode suffix but this is JDK verison specific so not asserting that in the test i.e. the test will break if we use different jdk
//        String topicWithNoMapping = "sdx.sei.hcd_test_stg.test_schemaA.noTableMapped";
//        assertThat(tm.getTable(topicWithNoMapping)).startsWith("sdx_sei_hcd_test_stg_test_schemaA_noTableMapped_");
//        assertThat(tm.getSchema(topicWithNoMapping)).isEqualTo("TEST_SCHEMA");
    }

    @Test
    public void testGetSchemaName(){
        tm.validate(connectorConfig, config); // TODO I think ths is wrong? check - does validate always get called
        MockSnowflakeSinkTask sinkTask = new MockSnowflakeSinkTask();
        tm.start(connectorConfig, sinkTask);

        assertGetSchema("app.prebill.test", "PREBILL_SCHEMA");
        assertGetSchema("app.prebill.test.one", "PREBILL_SCHEMA");
        assertGetSchema("app.prebill.test.one.two", "PREBILL_SCHEMA");

        assertGetSchema("app.pieces.test", "PIECES_SCHEMA");
        assertGetSchema("app.pieces.test.one", "PIECES_SCHEMA");
        assertGetSchema("app.pieces.test.one.two", "PIECES_SCHEMA");

        // no it doesn't default to the global schema if topic2Schema is non-null
        TopicPartition noMappingTopic = new TopicPartition("none", 0);
        assertThat( tm.getSchema(noMappingTopic) ).isNull();

        tm.stop(sinkTask);
    }

    private void assertGetSchema(String topic, String expectedSchema) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        assertThat( tm.getSchema(topicPartition) ).isEqualTo(expectedSchema);
    }


    public NatsSubjectMapping getUnvalidatedTopicMapping(){
        return tm;
    }

    public Config getConfig(){
        return config;
    }

    public NatsSubjectMapping getValidatedTopicMapping(){
        tm.validate(connectorConfig, config);
        return tm;
    }
}
