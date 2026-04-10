package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.*;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;
import static org.assertj.core.api.Assertions.*;

public class MultiSchemaTopicMappingTest {

    private Map<String,String> connectorConfig;
    private MultiSchemaTopicMapping tm;
    private Config config;

    @Before
    public void setupTopicMapping(){
        // use the 'getTopicMappingProvdier' factory method to create the MultiSchemaTopicMapping
        connectorConfig = new HashMap<>();
        connectorConfig.put(TOPIC_MAPPING_PROVIDER_CLASS, MultiSchemaTopicMapping.class.getName());
        TopicMapping topicMapping = SnowflakeSinkConnector.getTopicMappingProvider(connectorConfig);
        assertThat(topicMapping).isInstanceOf(MultiSchemaTopicMapping.class);
        tm = (MultiSchemaTopicMapping) topicMapping;

        connectorConfig.put(TOPICS_TABLES_MAP, "sdx.sei.hcd_test_stg.test_schemaA.kafka_events:KAFKA_EVENTS,sdx.sei.hcd_test_stg.test_schemaB.kafka_events:KAFKA_EVENTS2");
        connectorConfig.put(TOPIC_PREFIX_TO_SCHEMA_MAP, "sdx.sei.hcd_test_stg.test_schemaA:TEST_SCHEMA,sdx.sei.hcd_test_stg.test_schemaB:TEST_SCHEMA2");
        connectorConfig.put(SF_URL, "url");
        connectorConfig.put(SF_USER, "user");
        connectorConfig.put(SF_DATABASE, "database");
        connectorConfig.put(INGESTION_METHOD_OPT, SNOWPIPE_STREAMING.toString());
        updateConfig();
    }

    private void updateConfig() {
        // this is what SnowflakeSinkConnector does to create the Config
        ConfigDef configDef = ConnectorConfigDefinition.getConfig();
        config = new Config(configDef.validate(connectorConfig));
    }

    @Test
    public void testValidate(){
        boolean valid = tm.validate(connectorConfig, config);
        assertThat(getErrors(config)).isEmpty();
        assertThat(valid).isTrue();
    }

    @Test
    public void testValidate_globalSchemaSpecified(){
        connectorConfig.put(SF_SCHEMA, "schema"); // this shouldn't be specified when using multischema
        assertThat(tm.validate(connectorConfig, config)).isFalse();
        assertError(SF_SCHEMA, "snowflake.schema.name must not be used for multischema support.", config);
    }

    @Test
    public void testValidate_wrongIngestionMethod(){
        connectorConfig.put(INGESTION_METHOD_OPT, SNOWPIPE.toString()); // this shouldn't be specified when using multischema
        assertThat(tm.validate(connectorConfig, config)).isFalse();
        assertError(INGESTION_METHOD_OPT, "snowflake.ingestion.method Only SNOWPIPE_STREAMING is supported for multischema support.", config);
    }

    @Test
    public void testValidate_missingTopicPrefixMap(){
        connectorConfig.remove(TOPIC_PREFIX_TO_SCHEMA_MAP);
        updateConfig();
        assertThat( tm.validate(connectorConfig, config) ).isFalse();
        assertError(TOPIC_PREFIX_TO_SCHEMA_MAP, TOPIC_PREFIX_TO_SCHEMA_MAP + " must be provided", config);
    }

    @Test
    public void testValidate_schemaMapHasAValueWhichIsInvalid(){
        // the mapping is incomplete - there is no : in the last one
        connectorConfig.put(TOPIC_PREFIX_TO_SCHEMA_MAP, "sdx.sei.hcd_test_stg.test_schemaA:TEST_SCHEMA,sdx.sei.hcd_test_stg.test_schemaB"/*:TEST_SCHEMA2*/);

        // this should get caught by 'TopicToSchemaValidator' when the very first validation is done by ConfigDef.validate
        updateConfig();
        assertError(TOPIC_PREFIX_TO_SCHEMA_MAP, "Invalid value sdx.sei.hcd_test_stg.test_schemaA:TEST_SCHEMA,sdx.sei.hcd_test_stg.test_schemaB for configuration snowflake.topicPrefix2schema.map: Format: <topic-prefix-1>:<schema-1>,<topc-prefix-2>:<schema-2>,...", config);

        assertThat( tm.validate(connectorConfig, config) ).isFalse();
    }


    final String chcsaTopicToTable = "sdx.fhir.emr_data_raw.chsca.binaries_fhir:BINARIES_FHIR,sdx.fhir.emr_data_raw.chsca.diagnostic_reports_fhir:DIAGNOSTIC_REPORTS_FHIR,sdx.fhir.emr_data_raw.chsca.document_references_fhir:DOCUMENT_REFERENCES_FHIR,sdx.fhir.emr_data_raw.chsca.encounters_fhir:ENCOUNTERS_FHIR,sdx.fhir.emr_data_raw.chsca.medication_requests_fhir:MEDICATION_REQUESTS_FHIR,sdx.fhir.emr_data_raw.chsca.observations_fhir:OBSERVATIONS_FHIR,sdx.fhir.emr_data_raw.chsca.patients_fhir:PATIENTS_FHIR,sdx.fhir.emr_data_raw.chsca.procedures_fhir:PROCEDURES_FHIR,sdx.fhir.emr_data_raw.chsca.service_requests_fhir:SERVICE_REQUESTS_FHIR";
    final String arkbapTopicToTable = "sdx.fhir.emr_data_raw.arkbap.binaries_fhir:BINARIES_FHIR,sdx.fhir.emr_data_raw.arkbap.diagnostic_reports_fhir:DIAGNOSTIC_REPORTS_FHIR,sdx.fhir.emr_data_raw.arkbap.document_references_fhir:DOCUMENT_REFERENCES_FHIR,sdx.fhir.emr_data_raw.arkbap.encounters_fhir:ENCOUNTERS_FHIR,sdx.fhir.emr_data_raw.arkbap.medication_requests_fhir:MEDICATION_REQUESTS_FHIR,sdx.fhir.emr_data_raw.arkbap.observations_fhir:OBSERVATIONS_FHIR,sdx.fhir.emr_data_raw.arkbap.patients_fhir:PATIENTS_FHIR,sdx.fhir.emr_data_raw.arkbap.procedures_fhir:PROCEDURES_FHIR,sdx.fhir.emr_data_raw.arkbap.service_requests_fhir:SERVICE_REQUESTS_FHIR";

    @Test
    public void testRealExampleFromProd(){
        // let's combine two connectors - fhir-chsca-connector and fhir-arkbap-connector
        connectorConfig.put(TOPICS_TABLES_MAP, chcsaTopicToTable + "," + arkbapTopicToTable);
        connectorConfig.put(TOPIC_PREFIX_TO_SCHEMA_MAP, "sdx.fhir.emr_data_raw.chsca:CHSCA,sdx.fhir.emr_data_raw.arkbap:ARKBAP");
        updateConfig();
        assertNoErrors(config);
        tm.validate(connectorConfig, config);
        assertNoErrors(config);
    }


    // there is a topic in the topic2table but there is nothing matching that inside the schema mapping
    @Test
    public void testValidate_schemaMapHasAValueWhichIsValid_butMissingTopicMapping(){
        // let's combine two connectors - fhir-chsca-connector and fhir-arkbap-connector
        connectorConfig.put(TOPICS_TABLES_MAP, chcsaTopicToTable + "," + arkbapTopicToTable);
        // we forgot to setup the schema mapping for ARKBAP
        connectorConfig.put(TOPIC_PREFIX_TO_SCHEMA_MAP, "sdx.fhir.emr_data_raw.chsca:CHSCA"/*,sdx.fhir.emr_data_raw.arkbap:ARKBAP"*/);
        updateConfig();
        assertNoErrors(config); // it doesn't get cught in the first validation by ConfigDef (because each individual value is valid on its own)
        try {
            tm.validate(connectorConfig, config); // but it does get caught here
            fail("shouldn't reach here");
        }  catch (SnowflakeKafkaConnectorException ex){
            assertThat(ex.getCode()).isEqualTo(SnowflakeErrors.ERROR_0033.getCode());
        }
    }

    @Test
    public void testGetTopicPrefixToSchemaMap(){
        tm.start(connectorConfig);
        assertThat(tm.topicPrefix2Schema).hasSize(2).containsOnly(
                entry("sdx.sei.hcd_test_stg.test_schemaa", "TEST_SCHEMA"),
                entry("sdx.sei.hcd_test_stg.test_schemab", "TEST_SCHEMA2")
        );
    }

    @Test
    public void testGetTable(){
        tm.start(connectorConfig);
        assertThat(tm.getTable("sdx.sei.hcd_test_stg.test_schemaA.kafka_events")).isEqualTo("KAFKA_EVENTS");
        assertThat(tm.getTable("sdx.sei.hcd_test_stg.test_schemaB.kafka_events")).isEqualTo("KAFKA_EVENTS2");

        // if there is no mapping it should still generate a new table based on the topic name
        // - the generated name will have a hashcode suffix but this is JDK verison specific so not asserting that in the test i.e. the test will break if we use different jdk
        String topicWithNoMapping = "sdx.sei.hcd_test_stg.test_schemaA.noTableMapped";
        assertThat(tm.getTable(topicWithNoMapping)).startsWith("sdx_sei_hcd_test_stg_test_schemaA_noTableMapped_");
        assertThat(tm.getSchema(topicWithNoMapping)).isEqualTo("TEST_SCHEMA");
    }

    @Test
    public void testGetSchemaName(){
        tm.start(connectorConfig);
        TopicPartition topicPartition = new TopicPartition("sdx.sei.hcd_test_stg.test_schemaA.kafka_events", 0);
        TopicPartition topicPartition2 = new TopicPartition("sdx.sei.hcd_test_stg.test_schemaB.kafka_events", 0);
        assertThat( tm.getSchema(topicPartition) ).isEqualTo("TEST_SCHEMA");
        assertThat( tm.getSchema(topicPartition2) ).isEqualTo("TEST_SCHEMA2");
        // no it doesn't default to the global schema if topic2Schema is non-null
        TopicPartition noMappingTopic = new TopicPartition("none", 0);
        assertThat( tm.getSchema(noMappingTopic) ).isNull();
    }

    @Test
    public void testGetAllSchemas(){
        tm.start(connectorConfig);
        assertThat(tm.getAllSchemas()).containsOnly("TEST_SCHEMA", "TEST_SCHEMA2");
    }

    static void assertNoErrors(Config config){
        List<ConfigValue> errors = getErrors(config);
        assertThat(errors).isEmpty();
    }

    static void assertError(String key, String errorMessage, Config config){
        List<ConfigValue> errors = getErrors(config);
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).name()).isEqualTo(key);
        assertThat(errors.get(0).errorMessages().get(0)).isEqualTo(errorMessage);
    }

    static List<ConfigValue> getErrors(Config config){
        return config.configValues().stream().filter(c -> !c.errorMessages().isEmpty()).collect(Collectors.toList());
    }
}
