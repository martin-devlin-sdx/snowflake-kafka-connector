package com.snowflake.kafka.connector.internal.streaming.v2.migration;

import java.util.Locale;

/**
 * Controls whether the connector reads committed offsets from SSv1 channels during migration from
 * KC v3 to KC v4. Only consulted when the SSv2 channel has no committed offset yet.
 */
public enum Ssv1MigrationMode {
  /** Do not query SSv1 at all (default, current behavior). */
  SKIP,

  /** If SSv2 has no committed offset, query SSv1 and use its offset as the starting point. */
  MIGRATE,

  /**
   * If SSv2 has no committed offset, query SSv1. If SSv1 has an offset, fail the channel open so
   * the operator can investigate rather than silently migrating.
   */
  FAIL_ON_MISMATCH;

  /** Parses a config string into a migration mode, case-insensitive. Defaults to {@link #SKIP}. */
  public static Ssv1MigrationMode fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      return SKIP;
    }
    return valueOf(value.trim().toUpperCase(Locale.ROOT));
  }
}
