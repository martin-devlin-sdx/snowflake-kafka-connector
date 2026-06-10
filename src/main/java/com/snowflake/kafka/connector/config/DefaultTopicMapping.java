package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.SnowflakeSinkTask;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import org.apache.kafka.common.config.Config;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.TOPIC_PREFIX_TO_SCHEMA_MAP;
import static com.snowflake.kafka.connector.Utils.*;

/**
 * Use static configuration from the connector config. All topics map to the same schema.
 */
public class DefaultTopicMapping implements TopicMapping {

    private static final KCLogger LOGGER = new KCLogger(DefaultTopicMapping.class.getName());

    private Map<String, String> topic2Table;
    private String schema;

    @Override
    public boolean validate(Map<String, String> connectorConfig, Config config) {
        // check required fields aren't null
        if (!Utils.isSingleFieldValid(config, SF_URL, SF_USER, SF_DATABASE, SF_SCHEMA)){
            return false;
        }
        if (connectorConfig.containsKey(TOPIC_PREFIX_TO_SCHEMA_MAP)){
            Utils.updateConfigErrorMessage(config, TOPIC_PREFIX_TO_SCHEMA_MAP, " must not be used for for single. Use only " + SF_SCHEMA );
            return false;
        }
        Map<String, String> topic2Table = Utils.parseTopicToTableMap(connectorConfig.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP));
        if (topic2Table == null){ // normally it will throw exception when its invalid but it does return null in one case..
            return false;
        }
        init(connectorConfig); // 'schema' needs to get populated for the schema check during validation
        return true;
    }

    public void start(Map<String, String> connectorConfig, SnowflakeSinkTask task){
        init(connectorConfig);
    }

    private void init(Map<String, String> connectorConfig) {
        schema = connectorConfig.get(Utils.SF_SCHEMA);
        topic2Table = Utils.parseTopicToTableMap(connectorConfig.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP));
    }

    @Override
    public Set<String> getAllSchemas() {
        return Collections.singleton(schema);
    }

    @Override
    public String getSchema(String topic) {
        // use the same schema for all topics
        return schema;
    }

    @Override
    public String getTable(String topic) {
        return Utils.tableName(topic, topic2Table);
    }

    @Override
    public Map<String, String> getTopic2table() {
        return topic2Table;
    }
}
