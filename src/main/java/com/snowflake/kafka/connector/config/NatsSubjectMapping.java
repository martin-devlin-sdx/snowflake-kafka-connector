package com.snowflake.kafka.connector.config;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.*;
import com.snowflake.kafka.connector.internal.KCLogger;
import io.nats.client.*;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import org.apache.kafka.common.config.Config;

import java.util.*;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.*;
import static java.util.stream.Collectors.toList;

/**
 * Consume from "subjects" in NATS instead of consuming topics from kafka.
 *
 * In this code when we say 'topic' we really mean 'subject' in NATS not a physical topic in kafka.
 */
public class NatsSubjectMapping extends MultiSchemaTopicMapping {

    private static final KCLogger LOGGER = new KCLogger(NatsSubjectMapping.class.getName());

    // this is static because it's a singleton instance shared with all threads
    static NatsListenerService natsListenerService;

    public static synchronized NatsListenerService getNatsListenerService(Map<String, String> connectorConfig){
        if (natsListenerService == null){
            // note: ACTUAL_MAX_TASKS is only available after com.snowflake.kafka.connector.SnowflakeSinkConnector.taskConfigs is invoked so we
            //       have to instantiate NatsListenerService as late as possible after the SinkTask instance is created
            Options connectionOptions = getConnectionOptions(connectorConfig);
            int maxThreads = Integer.parseInt(connectorConfig.get(Utils.ACTUAL_MAX_TASKS));
            SubscriptionMapping subscriptionMapping = new DefaultSubscriptionMapping(connectorConfig);
            natsListenerService = new NatsListenerService(connectionOptions, maxThreads, subscriptionMapping);
        }
        return natsListenerService;
    }

    @Override
    protected String[] getRequiredFields() {
        return new String[]{SF_URL, SF_USER, SF_DATABASE, NATS_SUBJECT_PREFIX_TO_SCHEMA_MAP, NATS_TABLES_MAP, NATS_URL};
    }

    public static void log(String msg){
        LOGGER.info( "[[" + Thread.currentThread().getName() + "]] " + msg);
    }

    @Override
    public boolean validate(Map<String, String> connectorConfig, Config config) {
        log("validate started");
        if (connectorConfig.containsKey(TOPIC_PREFIX_TO_SCHEMA_MAP)) {
            Utils.updateConfigErrorMessage(config, TOPIC_PREFIX_TO_SCHEMA_MAP, " must not be used for NATS support. Instead use " + NATS_SUBJECT_PREFIX_TO_SCHEMA_MAP);
            return false;
        }
        if (connectorConfig.containsKey(TOPICS_TABLES_MAP)) {
            Utils.updateConfigErrorMessage(config, TOPICS_TABLES_MAP, " must not be used for NATS support. Instead use " + NATS_TABLES_MAP );
            return false;
        }

        // clone these values as they are needed by the superclass
        cloneConfig(connectorConfig, NATS_SUBJECT_PREFIX_TO_SCHEMA_MAP, TOPIC_PREFIX_TO_SCHEMA_MAP);
        cloneConfig(connectorConfig, NATS_TABLES_MAP,  TOPICS_TABLES_MAP);
        if ( !super.validate(connectorConfig, config) ) {
            return false;
        }
        log("validate finished");
        return true;
    }

    private void cloneConfig(Map<String, String> connectorConfig, String from, String to) {
        String value = connectorConfig.get(from);
        if (from.equals(NATS_TABLES_MAP)){
            // convert the NATS Multi-level wildcard into a regex
            value = value.replaceAll("\\.>", ".*");
        }
        connectorConfig.put(to, value);
    }

    @Override
    public void start(Map<String, String> connectorConfig, SnowflakeSinkTask task) {
        // clone these values as they are needed by the superclass TODO this sucks - revisit
        cloneConfig(connectorConfig, NATS_SUBJECT_PREFIX_TO_SCHEMA_MAP, TOPIC_PREFIX_TO_SCHEMA_MAP);
        cloneConfig(connectorConfig, NATS_TABLES_MAP,  TOPICS_TABLES_MAP);
        super.start(connectorConfig, task);
        getNatsListenerService(connectorConfig).createNatsListenerThread(task);
    }

    @Override
    public void stop(SnowflakeSinkTask task) {
        if (natsListenerService != null) {
            natsListenerService.stopNatsListenerThread(task);
        }
    }

    // TODO this needs to use the LOGGER
    private static final ErrorListenerConsoleImpl errorListener = new ErrorListenerConsoleImpl(){
        @Override
        public void exceptionOccurred(Connection conn, Exception exp) {
            super.exceptionOccurred(conn, exp);
            LOGGER.error("Exception: ", exp);
        }
    };

    private static final ConnectionListener connectionListener = new ConnectionListener() {
        @Override
        public void connectionEvent(Connection conn, Events type, Long time, String uriDetails){
            LOGGER.info("Connection event: " + type + " time: " + time + " uriDetails: " + uriDetails );
        }
        @Override
        public void connectionEvent(Connection conn, Events type) {
            connectionEvent(conn, type, null, null);
        }
    };

    static Options getConnectionOptions(Map<String, String> connectorConfig) {
        return new Options.Builder()
                .server(connectorConfig.get(Utils.NATS_URL))    // nats://nats.us-east-1.dev.smarterdx.net:4222
                .connectionListener(connectionListener)
                .errorListener(errorListener) // TODO fix the logger - this one uses stdout
                .build();
    }


    @VisibleForTesting
    static List<String> getSubscriptions(Map<String, String> connectorConfig) {
        Map<String,String> subject2TableMap = parseTopicToTableMap(connectorConfig.get(NATS_TABLES_MAP));
        return subject2TableMap.keySet().stream().sorted().collect(toList());
    }
}
