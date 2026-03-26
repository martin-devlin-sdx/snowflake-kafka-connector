package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.TopicToTableParser;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validates key value pairs in the format {@code <key-1>:<value-1>,<key-2>:<value-2>}. Values that
 * contain colons or commas (e.g. URLs) can be quoted: {@code url:"http://host:8084"}.
 *
 * <p>It doesn't validate the type of values, only making sure the format is correct.
 */
class CommaSeparatedKeyValueValidator implements ConfigDef.Validator {
  public CommaSeparatedKeyValueValidator() {}

  public void ensureValid(String name, Object value) {
    String s = (String) value;
    if (s != null && !s.isEmpty()) {
      try {
        TopicToTableParser.parseKeyValuePairs(s);
      } catch (IllegalArgumentException e) {
        throw new ConfigException(name, value, e.getMessage());
      }
    }
  }

  public String toString() {
    return "Comma-separated key-value pairs format:"
        + " <key-1>:<value-1>,<key-2>:\"<value-2>\",...";
  }
}
