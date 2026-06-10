package com.snowflake.kafka.connector;

import com.google.common.collect.Lists;
import com.snowflake.kafka.connector.config.NatsSubjectMapping;
import com.snowflake.kafka.connector.config.NatsSubjectMappingTest;
import com.snowflake.kafka.connector.config.TopicMapping;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import io.nats.client.Message;
import io.nats.client.Options;
import io.nats.client.Options.Builder;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static com.snowflake.kafka.connector.Utils.NATS_URL;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class NatsListenerServiceTest {

    private NatsListenerService nls;

    Options connectionOptions = new Builder().server("DummyURL").build();

    @Test @Ignore("Disabled on build machine because it needs a real nats server")
    public void testConsumingFromNats(){
        // two subscriptions and 4 threads means all threads listen to both subscriptions (but the message should only be delivered once)

        // reuse the setup configuration NatsTopicMappingTest
        NatsSubjectMappingTest test = new NatsSubjectMappingTest();
        test.setupTopicMapping();
        assertThat(test.connectorConfig.get(NATS_URL)).isEqualTo("nats://127.0.0.1:4222"); // local nats server needs to be running

        // simulate the validation happening
        // - clone connectorConfig becuase the validation process may change it
        NatsSubjectMapping tm = test.getUnvalidatedTopicMapping();
        tm.validate( new HashMap<>(test.connectorConfig), test.getConfig());

        // this connects to nats server on localhost - simulate two sinkTasks each with their own thread
        MockSnowflakeSinkTask snowflakeSinkTask1 = new MockSnowflakeSinkTask();
        snowflakeSinkTask1.start(test.connectorConfig);

        MockSnowflakeSinkTask snowflakeSinkTask2 = new MockSnowflakeSinkTask();
        snowflakeSinkTask2.start(test.connectorConfig);

        int duration = 120_000;
        delay(duration);

        snowflakeSinkTask1.stop();
        snowflakeSinkTask2.stop();

        delay(10_000);

    }

    private static void delay(int duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /*
    @Test
    public void testGetHeaders(){
        nls = new NatsListenerService(connectionOptions, 5, asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));

        // empty headers
        Headers headers = null;
        Message msg = new NatsMessage("subject", null, headers, "test".getBytes());
        assertThat(nls.toHeaders(msg)).isNull();

        headers = new Headers();  // still empty
        msg = new NatsMessage("subject", null, headers, "test".getBytes());
        assertThat(nls.toHeaders(msg)).isNull();

        headers.add("name", "Jimi Hendrix");
        msg = new NatsMessage("subject", null, headers, "test".getBytes());
        assertThat(nls.toHeaders(msg)).hasSize(1);
        assertThat(nls.toHeaders(msg).iterator().next().key()).isEqualTo("name");
        assertThat(nls.toHeaders(msg).iterator().next().value()).isEqualTo("Jimi Hendrix");

        // one header with two values
        headers.add("name", "Jimmy Page"); // page
        msg = new NatsMessage("subject", null, headers, "test".getBytes());
        assertThat(nls.toHeaders(msg)).hasSize(1);
        assertThat(nls.toHeaders(msg).iterator().next().key()).isEqualTo("name");
        assertThat(nls.toHeaders(msg).iterator().next().value()).isEqualTo("Jimi Hendrix,Jimmy Page");

        // two headers
        headers.add("address", "somewhere");
        msg = new NatsMessage("subject", null, headers, "test".getBytes());
        // sort the headers
        List<Header> headersList = Lists.newArrayList(nls.toHeaders(msg)).stream().sorted(comparing(Header::key)).collect(toList());
        assertThat(headersList).hasSize(2);
        assertThat(headersList.get(0).key()).isEqualTo("address");
        assertThat(headersList.get(0).value()).isEqualTo("somewhere");
        assertThat(headersList.get(1).key()).isEqualTo("name");
        assertThat(headersList.get(1).value()).isEqualTo("Jimi Hendrix,Jimmy Page");
    }

    @Test
    public void testGetThreadNumber(){
        final String originalThreadName = Thread.currentThread().getName();
        List<String> subscriptions = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        int maxThreads = 5;
        nls = new NatsListenerService(connectionOptions, maxThreads, subscriptions);

        Thread.currentThread().setName(NatsListenerService.THREAD_NAME_PREFIX + "0");
        assertThat( nls.getThreadNumber() ).isEqualTo(0);

        Thread.currentThread().setName(NatsListenerService.THREAD_NAME_PREFIX + "1");
        assertThat( nls.getThreadNumber() ).isEqualTo(1);

        Thread.currentThread().setName(NatsListenerService.THREAD_NAME_PREFIX + "2");
        assertThat( nls.getThreadNumber() ).isEqualTo(2);

        // tidy up
        Thread.currentThread().setName(originalThreadName);
    }

    @Test @Ignore // paritioning is currently off - see comments in com.snowflake.kafka.connector.NatsListenerService.getSubscriptionsForThisThread
    public void getSubscriptionsForThisThread() {
        // 10 subjects and 5 threads
        List<String> subscriptions = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        int maxThreads = 5;
        nls = new NatsListenerService(connectionOptions, maxThreads, subscriptions);
        assertThat(nls.subscriptions).hasSize(10);

        assertGroup(0, "a", "f");
        assertGroup(1, "b", "g");
        assertGroup(2, "c", "h");
        assertGroup(3, "d", "i");
        assertGroup(4, "e", "j");

        // 11 subjects and 5 threads
        subscriptions = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
        maxThreads = 5;
        nls = new NatsListenerService(connectionOptions, maxThreads, subscriptions);
        assertThat(nls.subscriptions).hasSize(11);

        assertGroup(0, "a", "f", "k");
        assertGroup(1, "b", "g");
        assertGroup(2, "c", "h");
        assertGroup(3, "d", "i");
        assertGroup(4, "e", "j");

        // 12 subjects and 5 threads
        subscriptions = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l");
        maxThreads = 5;
        nls = new NatsListenerService(connectionOptions, maxThreads, subscriptions);
        assertThat(nls.subscriptions).hasSize(12);

        assertGroup(0, "a", "f", "k");
        assertGroup(1, "b", "g", "l");
        assertGroup(2, "c", "h");
        assertGroup(3, "d", "i");
        assertGroup(4, "e", "j");

        // 9 subjects and 5 threads
        subscriptions = asList("a", "b", "c", "d", "e", "f", "g", "h", "i");
        maxThreads = 5;
        nls = new NatsListenerService(connectionOptions, maxThreads, subscriptions);
        assertThat(nls.subscriptions).hasSize(9);

        assertGroup(0, "a", "f");
        assertGroup(1, "b", "g");
        assertGroup(2, "c", "h");
        assertGroup(3, "d", "i");
        assertGroup(4, "e");
    }

    private void assertGroup(int threadNumber, String ... subjects) {
        assertThat(nls.getSubscriptionsForThisThread(threadNumber)).containsExactly(subjects);
    }
     */


    public static class MockSnowflakeSinkTask extends SnowflakeSinkTask {

        List<SinkRecord> sinkRecords = new ArrayList<>();
        List<TopicPartition> opened = new ArrayList<>();

        TopicMapping topicMapping;

        @Override
        public void start(Map<String, String> parsedConfig) {
            topicMapping = SnowflakeSinkConnector.getTopicMappingProvider(parsedConfig);
            topicMapping.start(parsedConfig, this);
        }

        @Override
        public void stop() {
            topicMapping.stop(this);
        }

        @Override
        public void open(Collection<TopicPartition> partitions) {
            opened.addAll(partitions);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            sinkRecords.addAll(records);
            records.forEach(System.out::println);
        }

        @Override
        public Optional<SnowflakeConnectionService> getSnowflakeConnection() {
            return Optional.empty();
        }

        @Override
        public SnowflakeSinkService getSink() {
            return null;
        }
    }
}
