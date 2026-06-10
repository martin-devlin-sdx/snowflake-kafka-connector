package com.snowflake.kafka.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.NatsMessage;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class SnowflakeSinkServiceForNATSTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testConnectingToSnowflakeUsing_usingSnowflakeStreamingIngestClient() throws ExecutionException, InterruptedException {
        Properties props = getProps();

        Mapping mapping = new Mapping("EMR_DATA_RAW_DEV", "MARTIN", "NATS_PREBILL_EVENTS");

        SnowflakeSinkServiceForNATS sink = new SnowflakeSinkServiceForNATS(props, mapping);
        assertThat(sink.snowflakeClient).isNotNull();
        assertThat(sink.snowflakeClient.isClosed()).isFalse();
        assertThat(sink.snowflakeClient.getName()).isEqualTo("MY_CLIENT");

        SnowflakeStreamingIngestChannel channel1 = sink.getChannel("test");
        assertThat(channel1).isNotNull();

        // test writing a record
        for (int index = 0; index < 10; index++){
            boolean inserted = sink.insertRecord(new MockMessage("app.prebill.test", "app.prebill.>", "This is my message number " + index));
            assertThat(inserted).isTrue();
        }

        // test writing to a different table
        mapping.schema = "MARTIN2";
        mapping.table = "OTHER_EVENTS2";

        // we changed in mapping in the mock so there should be a 2nd channel1 different from the first
        SnowflakeStreamingIngestChannel channel2 = sink.getChannel("test");
        assertThat(channel2).isNotEqualTo(channel1);
        assertThat(channel2.getName()).isEqualTo("CHANNEL.EMR_DATA_RAW_DEV.MARTIN2.OTHER_EVENTS2");

        for (int index = 0; index < 10; index++){
            boolean inserted = sink.insertRecord(new MockMessage("app.prebill.test", "app.prebill.>", "This is my message for MARTIN2.OTHER_EVENTS2 number " + index));
            assertThat(inserted).isTrue();
        }

        System.out.println("Before close");
        sink.close();
        System.out.println("After close");
    }



    private static @NonNull Properties getProps() {
        // don't hardcode the private key needed in the source code
        return toProperties("/Users/mdevlin/Documents/dev/snowflake-kafka-connector/profile2.json");
    }

    private static Properties toProperties(String jsonFile) {
        try {
            Properties props = new Properties();
            Iterator<Map.Entry<String, JsonNode>> propIt = mapper.readTree(new String(Files.readAllBytes(Paths.get(jsonFile)))).fields();
            while (propIt.hasNext()) {
                Map.Entry<String, JsonNode> prop = propIt.next();
                props.put(prop.getKey(), prop.getValue().asText());
            }
            return props;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




    public static class Mapping implements SubscriptionMapping {
        String database;
        String schema;
        String table;

        public Mapping(String database, String schema, String table){
            this.database = database;
            this.schema = schema;
            this.table = table;
        }

        @Override
        public String getSchema(String subject) {
            return schema;
        }

        @Override
        public String getTable(String subject) {
            return table;
        }

        @Override
        public String getDatabase(String subject) {
            return database;
        }

        @Override
        public Set<String> getAllSchemas() {
            return Collections.singleton(schema);
        }

        @Override
        public List<String> getAllSubscriptions() {
            return null;
        }
    }



    public static class MockMessage extends NatsMessage {
        final String subscription_;

        public MockMessage(String subject, String subscription, String body){
            this.subject = subject;
            this.data =  body.getBytes(StandardCharsets.UTF_8);
            this.subscription_ = subscription;
        }

        @Override
        public Subscription getSubscription() {
            return new Subscription() {
                @Override
                public String getSubject() {
                    return subscription_;
                }
                @Override
                public String getQueueName() {
                    return "";
                }
                @Override
                public Dispatcher getDispatcher() {
                    return null;
                }
                @Override
                public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
                    return null;
                }
                @Override
                public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
                    return null;
                }
                @Override
                public void unsubscribe() {
                }

                @Override
                public Subscription unsubscribe(int after) {
                    return null;
                }

                @Override
                public void setPendingLimits(long maxMessages, long maxBytes) {
                }

                @Override
                public long getPendingMessageLimit() {
                    return 0;
                }

                @Override
                public long getPendingByteLimit() {
                    return 0;
                }

                @Override
                public long getPendingMessageCount() {
                    return 0;
                }

                @Override
                public long getPendingByteCount() {
                    return 0;
                }

                @Override
                public long getDeliveredCount() {
                    return 0;
                }

                @Override
                public long getDroppedCount() {
                    return 0;
                }

                @Override
                public void clearDroppedCount() {
                }

                @Override
                public boolean isActive() {
                    return false;
                }

                @Override
                public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
                    return null;
                }
            };
        }
    }
}