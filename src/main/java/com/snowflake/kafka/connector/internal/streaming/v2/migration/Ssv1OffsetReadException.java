package com.snowflake.kafka.connector.internal.streaming.v2.migration;

/**
 * Thrown when reading an SSv1 committed offset fails due to a transient or unexpected error. The
 * caller must not silently proceed when this is thrown -- falling through to the Kafka consumer
 * group offset could cause duplicates if the (unreadable) SSv1 offset is ahead.
 */
public class Ssv1OffsetReadException extends RuntimeException {

  public Ssv1OffsetReadException(String message) {
    super(message);
  }

  public Ssv1OffsetReadException(String message, Throwable cause) {
    super(message, cause);
  }
}
