package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkTask;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.*;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.TOPIC_PREFIX_TO_SCHEMA_MAP;
import static com.snowflake.kafka.connector.SnowflakeSinkTask.getIngestionMethodConfig;
import static com.snowflake.kafka.connector.Utils.*;
import static com.snowflake.kafka.connector.Utils.SF_DATABASE;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;

public class MultiSchemaTopicMapping implements TopicMapping {

    private static final KCLogger LOGGER = new KCLogger(MultiSchemaTopicMapping.class.getName());

    Map<String, String> topic2Table;
    Map<String, String> topicPrefix2Schema;

    @Override
    public boolean validate(Map<String, String> connectorConfig, Config config) {
        // check required fields aren't null
        if (!Utils.isSingleFieldValid(config, getRequiredFields())) {
            return false;
        }
        if (connectorConfig.containsKey(SF_SCHEMA)) {
            Utils.updateConfigErrorMessage(config, SF_SCHEMA, " must not be used for multischema support.");
            return false;
        }
        // multi-schema support is only supported for SNOWPIPE_STREAMING not SNOWPIPE classic
        if (getIngestionMethodConfig(connectorConfig) != SNOWPIPE_STREAMING) {
            Utils.updateConfigErrorMessage(config, INGESTION_METHOD_OPT, " Only SNOWPIPE_STREAMING is supported for multischema support.");
            return false;
        }
        if (Utils.isIcebergEnabled(connectorConfig)) {
            Utils.updateConfigErrorMessage(config, ICEBERG_ENABLED, " Multischema support does not handle Iceberg streaming");
            return false;
        }
        if (Utils.isSchematizationEnabled(connectorConfig)) {
            // schematization probably does work but I'm treating this as outside scope of testing for our immediate needs - Martin D.
            // - see com.snowflake.kafka.connector.internal.streaming.DirectTopicPartitionChannel.handleInsertRowFailure
            Utils.updateConfigErrorMessage(config, ENABLE_SCHEMATIZATION_CONFIG, " Multischema support does not handle schematization");
            return false;
        }
        Map<String,String> topic2Table = Utils.parseTopicToTableMap(connectorConfig.get(TOPICS_TABLES_MAP));
        if (topic2Table == null) { // normally it will throw exception when its invalid but it does return null in one case..
            return false;
        }
        // this might throw an exception if there is a missing schema definition for a topic defined inside topic2Table
        parseTopicPrefixToSchemaMap(connectorConfig.get(TOPIC_PREFIX_TO_SCHEMA_MAP), topic2Table);

        init(connectorConfig); // need to setup 'topicPrefix2Schema' to make getAllSchemas() work during validation
        return true;
    }

    protected String[] getRequiredFields(){
        return new String[]{SF_URL, SF_USER, SF_DATABASE, TOPIC_PREFIX_TO_SCHEMA_MAP};
    }


    @Override
    public void start(Map<String, String> connectorConfig, SnowflakeSinkTask task) {
        init(connectorConfig);
    }

    private void init(Map<String, String> connectorConfig) {
        topic2Table = Utils.parseTopicToTableMap(connectorConfig.get(TOPICS_TABLES_MAP));
        topicPrefix2Schema = parseTopicPrefixToSchemaMap(connectorConfig.get(TOPIC_PREFIX_TO_SCHEMA_MAP), topic2Table);
    }

    @Override
    public Set<String> getAllSchemas() {
        return new HashSet<>(topicPrefix2Schema.values());
    }

    @Override
    public String getSchema(String topic) {
        return getSchemaForTopicFromSchemaMap(topic,  topicPrefix2Schema);
    }

    @Override
    public String getTable(String topic) {
        return Utils.tableName(topic, topic2Table);
    }

    @Override
    public Map<String, String> getTopic2table() {
        return topic2Table;
    }

    private static Map<String, String> parseTopicPrefixToSchemaMap(String input, Map<String, String> topic2Table) {
       return parseTopicPrefixToSchemaMap(input, topic2Table, TOPIC_PREFIX_TO_SCHEMA_MAP, SnowflakeErrors.ERROR_0033) ;
    }
    public static Map<String, String> parseTopicPrefixToSchemaMap(String input, Map<String, String> topic2Table, String configName, SnowflakeErrors invalidErrorCode) {
        if (input == null || input.trim().isEmpty()) {
            throw invalidErrorCode.getException();
        }
        Map<String, String> topicPrefixToSchemaMap = new HashMap<>();
        boolean isInvalid = false;
        for (String str : input.split(",")) {
            String[] parts = str.split(":");

            if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
                LOGGER.error("Invalid {} config format: {}", configName, input);
                return null;
            }

            String topicPrefix = parts[0].trim();
            String schema = parts[1].trim();
            // TODO validate more - what is are the schema name constraints?

            if (topicPrefixToSchemaMap.containsKey(topicPrefix)) {
                LOGGER.error("topic prefix {} is duplicated in {}", topicPrefix, configName);
                isInvalid = true;
            }

            // check that prefixes don't overlap
            for (String parsedSchemaPrefix : topicPrefixToSchemaMap.keySet()) {
                if (parsedSchemaPrefix.startsWith(topicPrefix) || topicPrefix.startsWith(parsedSchemaPrefix)) {
                    LOGGER.error("topic prefix cannot overlap: {}, {} in {}", parsedSchemaPrefix, topicPrefix, configName);
                    isInvalid = true;
                }
            }
            topicPrefixToSchemaMap.put(topicPrefix.toLowerCase(), schema);
        }
        if (topic2Table != null) { // when this method invoked from TopicToSchemaValidator this is not available yet...
            // validate that there is an entry matching for every topic in the topic2Table map
            for (String topic : topic2Table.keySet()) {
                String schema = getSchemaForTopicFromSchemaMap(topic, topicPrefixToSchemaMap);
                if (schema == null) {
                    isInvalid = true;
                    LOGGER.error("missing schema for topic: {} in {}", topic, configName);
                }
            }
        }
        if (isInvalid) {
            throw invalidErrorCode.getException();
        }
        return topicPrefixToSchemaMap;
    }

    /**
     * @param topic
     * @param topicPrefixToSchemaMap pre: must not be null
     * @return schema for this topic
     */
    public static String getSchemaForTopicFromSchemaMap(String topic, Map<String, String> topicPrefixToSchemaMap) {
        topic = topic.toLowerCase();
        // first look for an exact match
        String schema = topicPrefixToSchemaMap.get(topic);
        if (schema == null) {
            // look for a prefix match
            // TODO reconsider - maybe we should be using regex match 'matches' here instead of 'startsWith'
            schema = topicPrefixToSchemaMap.keySet().stream().filter(topic::startsWith).findFirst().map(topicPrefixToSchemaMap::get).orElse(null);
        }
        return schema;
    }

    /**
     * Used by the config definition inside {@link ConnectorConfigDefinition}
     */
    public static class TopicToSchemaValidator implements ConfigDef.Validator {

        public static final String FORMAT = "Format: <topic-prefix-1>:<schema-1>,<topc-prefix-2>:<schema-2>,...";

        public void ensureValid(String name, Object value) {
            String s = (String) value;
            if (s != null && !s.isEmpty()) // this value is optional and can be empty
            {
                if (parseTopicPrefixToSchemaMap(s, null) == null) {
                    throw new ConfigException(name, value, FORMAT);
                }
            }
        }
        public String toString() {
            return "Topic to schema map format : comma-separated tuples, e.g. " + FORMAT;
        }
    }
}
