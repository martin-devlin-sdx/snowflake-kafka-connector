package com.snowflake.kafka.connector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import io.nats.client.*;
import io.nats.client.impl.Headers;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singleton;

/**
 * Listen to messages from NATS. For each message, invoke the {@link SnowflakeSinkTask#put(Collection)} callback to
 * forward these messages into snowflake.
 */
public class NatsListenerService {
    private static final KCLogger LOGGER = new KCLogger(NatsListenerService.class.getName());

    static final String THREAD_NAME_PREFIX = "NatsListenerService-startup-nats-connection";

    /**
     * Treat all nats subscriptions as queues. This means that the NATS messages will be delivered once, distributed across
     * multiple threads which may be across multiple connector worker nodes.
     */
    private static final String NATS_QUEUE = "nats-to-snowflake-connector";

    private final Options natsConnectionOptions;

    private final int maxThreads;
    private final SubscriptionMapping subscriptionMapping;

    @VisibleForTesting
    ExecutorService executorServiceForStartingNatsConnections;

    /**
     * Simulate a unique "kafkaOffset". We just want this number to keep increasing over time as we write messages to snowflake.
     * If this process gets restarted, we want to ensure the number used is greater than the previously used number.
     * Use time in nanoseconds since epoch as the starting point for a new process.
     * TODO this doesn't handle a cluster of NatsListenerService instances.
     */
    private final AtomicLong offset = new AtomicLong(System.currentTimeMillis() * 1_000_000);

    private final Map<SnowflakeSinkTask, NatsListener> natsListeners = new ConcurrentHashMap<>();

    public NatsListenerService(Options natsConnectionOptions, int maxThreads, SubscriptionMapping subscriptionMapping) {
        this.natsConnectionOptions = natsConnectionOptions;
        this.maxThreads = maxThreads;
        this.subscriptionMapping = subscriptionMapping;
        LOGGER.info("Creating NatsListenerService url: " + natsConnectionOptions.getNatsServerUris()  + "  maxThreads: " + maxThreads);
        // create shared thread pool which will be used by each SnowflakeSinkTask instance (one thread per SnowflakeSinkTask)
        executorServiceForStartingNatsConnections = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_PREFIX + "%d").build());
    }

    /**
     * Create a nats listener thread for this 'task'
     */
    public void createNatsListenerThread(SnowflakeSinkTask task){
        log("Creating NatsListenerThread");
        // launch a thread which does the actual listening to NATS
        NatsListener natsListener = new NatsListener(task);
        natsListeners.put(task, natsListener);
        // start the connection in a background thread. This thread is short-lived - once the connection is setup it exists quickly.
        // The NATS Dispatcher object encapsulates its own thread for doing the real work.
        Future<?> future = executorServiceForStartingNatsConnections.submit(natsListener);
    }

    public void stopNatsListenerThread(SnowflakeSinkTask task){
        NatsListener natsListener = natsListeners.remove(task);
        if (natsListener != null){
            natsListener.stop();
        }
    }


    public class NatsListener implements Runnable, MessageHandler {

        final private SnowflakeSinkTask snowflakeSinkTask;
        final private Set<String> openedTopics = new HashSet<>();
        private List<String> subscriptionsForThisConnection = new ArrayList<>();

        private Connection natsConnection;
        private Dispatcher natsDispatcher;

        private SnowflakeSinkServiceForNATS snowflakeSinkServiceForNATS;

        NatsListener(SnowflakeSinkTask snowflakeSinkTask) {
            this.snowflakeSinkTask = snowflakeSinkTask;
        }

        /**
         * Create the connection to the NATS server and setup the subscriptions. Note that this thread exists
         * after startup because the NATS "Dispatcher" encapsulates its own thread in the background. That thread is
         * the one which does the real work invoking {@link #onMessage(Message)} repeatedly.
         */
        @Override
        public void run() {
            log("NatsListener::run - started");
            // first wait to ensure the startup process has completed establishing connection to snowflake
            snowflakeSinkTask.getSnowflakeConnection(); // blocks for up to one minute
            snowflakeSinkTask.getSink(); // blocks for up to one minute
            log("NatsListener::run - after getSink()");

            SnowflakeStreamingIngestClient streamingIngestClient = ((SnowflakeSinkServiceV2) snowflakeSinkTask.getSink()).getStreamingIngestClient();
            snowflakeSinkServiceForNATS = new SnowflakeSinkServiceForNATS(streamingIngestClient, subscriptionMapping);

            // open a dedicated NATS connection for this SnowflakeSinkTask
            try  {
                natsConnection = Nats.connect(natsConnectionOptions);
                log("NatsListener::run - natsConnection");

                // 'Dispatcher' encapsulates a thread and tcp socket to the NATS server
                // Note: this current thread just launches the dispatcher thread which does the real work then monitors
                //        the SnowflakeSinkTask waiting for it to be closed, then shuts down the Dispatcher thread
                natsDispatcher = natsConnection.createDispatcher(this);
                log("NatsListener::run - natsDispatcher");

                // dispatcher.setPendingLimits(100, 1024 * 128);  - TODO revisit - maybe the defaults are just fine.

                // subscribe to the relevant subjects
                final int threadId = getThreadNumber();
                log("NatsListener::run - threadId:" + threadId);
                subscriptionsForThisConnection = getSubscriptionsForThisThread(threadId);
                LOGGER.info("The NATS subscriptions for this thread are: " + subscriptionsForThisConnection);
                for (String subscription : subscriptionsForThisConnection){
                    natsDispatcher.subscribe(subscription, NATS_QUEUE);
                    log("NatsListener::run - subscription:" + subscription);
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Callback for each message received from NATS. This is invoked by the NATS Dispatcher thread.
         */
        @Override
        public void onMessage(Message msg) {
            snowflakeSinkServiceForNATS.insertRecord(msg);
                /*
            String topic = toTopic(msg);
            log("NatsListener::onMessage for subject " + topic + " from subscription " + msg.getSubscription().getSubject());
            if (!openedTopics.contains(topic)) {
                // strictly speaking explicitly invoking 'open' here isn't necessary because it will automatically open it if needed
                // - but it will print an annoying WARN to the log in that case... Let's avoid that by using an explicit 'open'
                TopicPartition partition = new TopicPartition(topic, DEFAULT_PARTITION);
                log("NatsListener::open " + partition);
                snowflakeSinkTask.open(singleton(partition));
                openedTopics.add(topic);
            }
            SinkRecord sinkRecord = toSinkRecord(msg);
            log("NatsListener::onMessage sinkRecord:" + sinkRecord);
//            snowflakeSinkTask.put(singleton(sinkRecord));
            snowflakeSinkTask.getSink().insert(singleton(sinkRecord));
                 */
        }

        /**
         * disconnect from NATS server
         */
        public void stop(){
            log("NatsListener::stop");
            // shutting down so unsubscribe from all subjects
            for (String subscription : subscriptionsForThisConnection){
                try {
                    log("NatsListener::stop unsubscribe " + subscription);
                    natsDispatcher.unsubscribe(subscription);
                } catch (Exception ex){
                    LOGGER.info("Error unsubscribing NATS subscription: " + subscription, ex);
                }
            }
            // shutdown the connection
            log("NatsListener::stop - closing connection");
            closeQuietly(natsConnection);
        }

    }

    public static void log(String msg){
        LOGGER.info( "[[" + Thread.currentThread().getName() + "]] " + msg);
    }

    /**
     * TODO need to fix - this is problematic because offset tracking is per topcPartition / multiple threads can be consuming from
     * same subject and must have unique offsets....
     */
    private static final int DEFAULT_PARTITION = 0;


    private SinkRecord toSinkRecord(Message msg) {
        long kafkaOffset = this.offset.getAndIncrement();
        long timestampMs = System.currentTimeMillis();
        Iterable<Header> headers = toHeaders(msg);
        byte[] value = msg.getData(); // TODO revisit - should be convert this to string? What about protobuffer etc.? Unfortunately 'msg' doesn't have any schema info about the payload
        return new SinkRecord(toTopic(msg), DEFAULT_PARTITION, null, null, Schema.BYTES_SCHEMA, value,
                kafkaOffset, timestampMs, TimestampType.CREATE_TIME, headers);
    }

    private static String toTopic(Message msg) {
        return msg.getSubject();
    }

    /**
     * Convert NATS headers into kafka headers
     */
    Iterable<Header> toHeaders(Message msg) {
        if (!msg.hasHeaders()) {
            return null;
        }
        Headers natsHeaders = msg.getHeaders();
        ConnectHeaders kafkaHeaders = new ConnectHeaders();
        natsHeaders.forEach( (key, values)->{
            // TODO revisit - kafka may support multiple headers with same name so maybe we don't have to use comma delimited list
            String value = String.join(",", values);
            kafkaHeaders.add(key, value, Schema.STRING_SCHEMA);
        });
        return kafkaHeaders;
    }

    /**
     * Partition the NATS subscriptions across the task threads. Each thread has its own NATS connection so we are trying
     * to distribute the IO across each one. Of course there is no guarantee that the traffic is anywhere even across the
     * subscriptions in the first place so this may be rather pointless. i.e. one thread could still end up with 90% of IO traffic
     * while other threads are mostly idle.
     * @param threadNumber from 0 to {@link #maxThreads} - 1
     */
    List<String> getSubscriptionsForThisThread(int threadNumber){
        // TODO this isn't correct because the tasks.max in kafka connect is the total number of worker threads across
        // all worker nodes not just for the current JVM. For now don't partition...
        return subscriptionMapping.getAllSubscriptions();
//        assert threadNumber < maxThreads;
//        // if the number of subscriptions is small (less than maxThreads) just assign everything to every thread
//        // i.e. avoid completely idle thread .This is unlikely because the maxTasks tends to be small e.g. 4
//        if (subscriptions.size() < maxThreads){
//            return subscriptions;
//        }
//        // partition across the threads
//        List<String> subs = new ArrayList<>();
//        for (int index = threadNumber; index < subscriptions.size(); index += maxThreads){
//            subs.add(subscriptions.get(index));
//        }
//        return subs;
    }

    /**
     * @return a number from 0 up to {@link #maxThreads} - 1
     */
    int getThreadNumber(){
        String id = Thread.currentThread().getName().substring(THREAD_NAME_PREFIX.length());
        return Integer.parseInt(id);
    }

    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ex) {
            LOGGER.info("Exception while closing NATS connection: " + closeable, ex);
        }
    }
}
