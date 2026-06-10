package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.streaming.*;
import io.nats.client.Message;
import net.snowflake.ingest.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption.CONTINUE;
import static net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption.SKIP_BATCH;

/**
 * Insert messages that came from NATS into Snowflake.
 *
 * This is the analogue of {@link SnowflakeSinkServiceV2} but it has all kafka message dependencies removed.
 */
public class SnowflakeSinkServiceForNATS implements Closeable {

    public static final String QUOTE = "\"";

    private final Logger log = LoggerFactory.getLogger(SnowflakeSinkServiceForNATS.class);

    final SnowflakeStreamingIngestClient snowflakeClient;
    boolean externalClient;

    private final SubscriptionMapping subscriptionMapping;

    /**
     * nats subscription name --> channel
     */
    private final Map<String, SnowflakeStreamingIngestChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> channelOffsets = new ConcurrentHashMap<>();

    /**
     * Create SnowflakeStreamingIngestClient using ingest client properties
     */
    public SnowflakeSinkServiceForNATS(Properties ingestConfig, SubscriptionMapping subscriptionMapping){
        this( SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(ingestConfig).build(),  subscriptionMapping);
        this.externalClient = false;
    }

    public SnowflakeSinkServiceForNATS(SnowflakeStreamingIngestClient snowflakeClient, SubscriptionMapping subscriptionMapping){
        this.snowflakeClient = snowflakeClient;
        this.subscriptionMapping = subscriptionMapping;
        this.externalClient = true;
    }

    /**
     * @return true if it was successfully inserted
     */
    public boolean insertRecord(Message msg){
        String subscription = msg.getSubscription().getSubject();
        SnowflakeStreamingIngestChannel channel = getChannel(subscription);
        // if (msg instanceof NatsMessage){
        //    long offsettoken = ((NatsMessage)msg).metaData().streamSequence();
        // }
        String offsetToken = getNextOffsetToken(channel);
        log.info("Inserting record to channel {} with offsetToken {}", channel.getName(),  offsetToken);
        InsertValidationResponse response = channel.insertRow(toRow(msg), offsetToken);// TODO deal with offsetToken if needed for jetstream
        // TODO handle error properly - for now just print it
        if (response.hasErrors()){
            for (InsertValidationResponse.InsertError error : response.getInsertErrors()){
                log.error("Unable to insert record to channel {} with offsetToken {}", channel.getName(), offsetToken, error.getException());
            }
            return false;
        }
        return true;
    }

    private String getNextOffsetToken(SnowflakeStreamingIngestChannel channel) {
        String channelName = channel.getName();
        return String.valueOf(channelOffsets.get(channelName).incrementAndGet());
    }

    private Map<String,Object> toRow(Message msg) {
        Map<String,Object> row = new HashMap<>();
        // TODO fix - add headers etc.
        row.put("RECORD_METADATA", "{\n" +
                "  \"CreateTime\": 1776726453746,\n" +
                "  \"SnowflakeConnectorPushTime\": " + System.currentTimeMillis() + ",\n" +
                "  \"headers\": " + getHeaders(msg) +
                "  \"subject\": \"" + msg.getSubject() + "\"\n" +
                "}");
        row.put("RECORD_CONTENT", "{\"message\":\"" + new String(msg.getData()) + "\"}" ); // TODO need to handle different data types
        return row;
    }

    private String getHeaders(Message msg){
        StringBuffer buf = new  StringBuffer();
        buf.append("{\n");
        if (msg.hasHeaders()) {
            msg.getHeaders().forEach((k, v) -> {
                buf.append(QUOTE).append(k).append(QUOTE).append(": ");
                buf.append(QUOTE).append(v.get(0)).append(QUOTE).append("\n"); // TODO handle multiple header values
            });
        }
        buf.append("},\n");
        return buf.toString();
    }

    SnowflakeStreamingIngestChannel getChannel(String subscription) {
        final String channelName = getChannelName(subscription);
        SnowflakeStreamingIngestChannel channel = channels.computeIfAbsent(channelName, this::createChannel);
        if (channel.isClosed()){
            channel = createChannel(channelName);
            channels.put(channelName, channel);
        }
        return channel;
    }

    private SnowflakeStreamingIngestChannel createChannel(String subscription) {
        String table = subscriptionMapping.getTable(subscription);
        String db = subscriptionMapping.getDatabase(subscription);
        String schema = subscriptionMapping.getSchema(subscription);
        String channelName = getChannelName(db, schema, table);
        return createChannel(channelName, db, schema, table, false);
    }

    @Override
    public void close() {
        try {
            if (!externalClient){
                // the channels close asynchronously in the background TODO revisit
                //channels.values().forEach(SnowflakeStreamingIngestChannel::close);
                snowflakeClient.close();
            }
        } catch (Exception ex){
            log.error("Exception while closing SnowflakeStreamingIngestClient", ex);
        }
    }

    /**
     * Open a channel for Table with given channel name and tableName.
     * @return new channel
     */
    private SnowflakeStreamingIngestChannel createChannel(String channelName, String db, String schema, String table, boolean schemaEvolutionEnabled) {
        // SKIP_BATCH is necessary to avoid race condition in the schematization flow
        OpenChannelRequest.OnErrorOption onErrorOption = schemaEvolutionEnabled ? SKIP_BATCH : CONTINUE;

        OpenChannelRequest channelRequest =
                OpenChannelRequest.builder(channelName)
                        .setDBName(db)
                        .setSchemaName(schema)
                        .setTableName(table)
                        .setOnErrorOption(onErrorOption)
//                        .setOffsetTokenVerificationFunction(StreamingUtils.offsetTokenVerificationFunction)
                        .build();
        log.info("Opening a channel with name:{}", channelName );
        try {
            // TODO this still uses kafka-snowflake-connector api - get rid of it.
            SnowflakeStreamingIngestChannel channel = OpenChannelRetryPolicy.executeWithRetry(() -> snowflakeClient.openChannel(channelRequest), channelName);
            assert channelName.equals(channel.getName()); // ensure what comes back is uppercase
            channelOffsets.computeIfAbsent(channelName, k -> new AtomicLong(System.currentTimeMillis() * 1_000_000));
            return channel;
        } catch (RuntimeException e) {
            log.error("Failed to open channel {} after retries: {}", channelName, e.getMessage(), e);
            // rethrow the original exception when retry limit exceeded
            throw e;
        }
    }

    private String getChannelName(String subscription){
        String db = subscriptionMapping.getDatabase(subscription);
        String schema = subscriptionMapping.getSchema(subscription);
        String table = subscriptionMapping.getTable(subscription);
        return getChannelName(db, schema, table);
    }

    private String getChannelName(String db, String schema, String table) {
        // use uppercase because the ChannelName that comes back from snowflake will be in uppercase and we use
        // SnowflakeStreamingIngestChannel.getName() sometimes to avoid haveing to keep reclaculating this
        return "CHANNEL." + db.toUpperCase() + "." + schema.toUpperCase() + "." + table.toUpperCase();
    }
}
