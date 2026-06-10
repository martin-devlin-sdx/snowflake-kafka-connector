package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.SnowflakeSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.Config;

import java.util.Map;
import java.util.Set;

/**
 * Strategy for encapsulating the configuration logic for topicName -> table and
 * topicName -> schema mapping configuration.
 */
public interface TopicMapping {
    /**
     * Validate the global configuration.
     *
     * @param connectorConfig the statically defined connector configuration
     * @param config - if there are validation errors they should be reported inside this object
     * @returns true if the configuration is valid, false if there is a config problem.
     */
    boolean validate(Map<String, String> connectorConfig, Config config);

    /**
     * Initialize the TopicMapping. This must be invoked before any of the getters.
     */
    void start(Map<String, String> connectorConfig, SnowflakeSinkTask task);

    /**
     * Stop the TopicMapping.
     */
    default void stop(SnowflakeSinkTask task) {
    }

    /**
     * @return all schemas used by this configuration. This allows the connector to validate that all schemas exist upon startup.
     */
    Set<String> getAllSchemas();

    String getSchema(String topic);

    String getTable(String topic);

    default String getSchema(TopicPartition topicPartition){
        return getSchema(topicPartition.topic());
    }

    default String getTable(TopicPartition topicPartition){
        return getTable(topicPartition.topic());
    }

    // TODO remove eventually - was trying to encapsulate this entirely. for now it is still exposed.
    Map<String, String> getTopic2table();
}
