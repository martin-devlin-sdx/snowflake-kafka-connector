package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.KCLogger;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class SnowflakeSinkTaskNoCommitOffsets extends SnowflakeSinkTask{
    private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkTaskNoCommitOffsets.class.getName());

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) throws RetriableException {
        LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::preCommit");
        return Collections.emptyMap();
    }

    @Override
    public void initialize(SinkTaskContext context) {
        LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::initialize");
        super.initialize(new IgnoreOffsets(context));
    }

    class IgnoreOffsets implements SinkTaskContext {
        SinkTaskContext delegate;

        IgnoreOffsets(SinkTaskContext context) {
            this.delegate = context;
        }

        @Override
        public Map<String, String> configs() {
            return delegate.configs();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            // no-op
            LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::offsets - ignoring");
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::offset - ignoring");
        }

        @Override
        public void timeout(long timeoutMs) {
            delegate.timeout(timeoutMs);
        }

        @Override
        public Set<TopicPartition> assignment() {
            return delegate.assignment();
        }

        @Override
        public void pause(TopicPartition... partitions) {
            LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::pause - ignoring");
            // no op
        }

        @Override
        public void resume(TopicPartition... partitions) {
            LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::resume - ignoring");
            // no op
        }

        @Override
        public void requestCommit() {
            LOGGER.info("SnowflakeSinkTaskNoCommitOffsets::requestCommit - ignoring");
            // no op
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return delegate.errantRecordReporter();
        }
    }
}
