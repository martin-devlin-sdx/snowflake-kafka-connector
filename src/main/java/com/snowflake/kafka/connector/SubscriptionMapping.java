package com.snowflake.kafka.connector;

import java.util.List;
import java.util.Set;

/**
 * Encapsulate the configuration of NATS subjects -> events table mapping.
 */
public interface SubscriptionMapping {

    String getSchema(String subject);

    String getTable(String subject);

    String getDatabase(String subject);

    /**
     * @return all schemas used by this configuration. This allows the connector to validate that all schemas exist upon startup.
     */
    Set<String> getAllSchemas();

    List<String> getAllSubscriptions();
}
