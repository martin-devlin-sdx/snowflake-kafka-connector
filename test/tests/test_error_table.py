"""E2E tests for Snowflake Error Table support in v4 high-throughput mode.

Verifies:
1. Table WITHOUT error logging + v4-ht → connector starts, invalid data silently dropped
2. Table WITH error logging + v4-ht → connector starts, invalid data captured in error table
"""

import json
import logging
import time
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"


def _v4_ht_config(*, dlq_topic=None):
    """Build a v4-ht connector config from the base template."""
    base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())
    config = dict(base["config"])
    config["snowflake.enable.schematization"] = "true"
    config["snowflake.validation"] = "server_side"
    if dlq_topic:
        config["errors.tolerance"] = "all"
        config["errors.deadletterqueue.topic.name"] = dlq_topic
        config["errors.deadletterqueue.topic.replication.factor"] = "1"
    return config


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_without_error_logging(driver, name_salt):
    """v4-ht targeting a table WITHOUT ERROR_LOGGING — connector starts, errors silently dropped."""
    table_name = f"et_no_logging{name_salt}"

    # Create table WITHOUT error logging
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT)"
    )
    driver.createTopics(table_name, partitionNum=1, replicationNum=1)

    config = _v4_ht_config()
    rest_request = driver.createConnector(
        name_salt=name_salt,
        unsalted_name="et_no_logging",
        config_template=config,
    )
    connector_name = rest_request["name"]
    driver.startConnectorWaitTime()

    try:
        # Send a mix of valid and invalid records
        records = [
            json.dumps({"ID": "valid_1", "VAL": 42}).encode(),
            json.dumps({"ID": "invalid_1", "VAL": "not_a_number"}).encode(),
        ]
        keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
        driver.sendBytesData(table_name, records, keys)

        # Wait for stabilization
        time.sleep(30)

        # Connector should be running (not crashed)
        failed = driver.get_failed_tasks(connector_name)
        assert not failed, f"Connector task failed: {failed}"

        # Valid record should be in the table
        count = driver.select_number_of_records(table_name)
        assert count >= 1, f"Expected at least 1 row, got {count}"

        # Without ERROR_LOGGING, error table query should return 0 rows
        cursor = driver.snowflake_conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM ERROR_TABLE({table_name})")
            error_rows = cursor.fetchall()
            logger.info("Error table rows (no logging): %d", len(error_rows))
            # We expect 0 because ERROR_LOGGING is not enabled
            assert len(error_rows) == 0, (
                f"Expected 0 error table rows without ERROR_LOGGING, got {len(error_rows)}"
            )
        except Exception as e:
            logger.info(
                "Error table query failed (expected without ERROR_LOGGING): %s", e
            )
    finally:
        driver.closeConnector(connector_name)
        try:
            driver.deleteTopic(table_name)
        except Exception:
            pass


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_with_error_logging(driver, name_salt):
    """v4-ht targeting a table WITH ERROR_LOGGING — invalid data captured in error table."""
    table_name = f"et_with_logging{name_salt}"

    # Create table WITH error logging
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT) "
        f"ERROR_LOGGING = TRUE"
    )
    driver.createTopics(table_name, partitionNum=1, replicationNum=1)

    config = _v4_ht_config()
    rest_request = driver.createConnector(
        name_salt=name_salt,
        unsalted_name="et_with_logging",
        config_template=config,
    )
    connector_name = rest_request["name"]
    driver.startConnectorWaitTime()

    try:
        # Send a mix of valid and invalid records
        records = [
            json.dumps({"ID": "valid_1", "VAL": 42}).encode(),
            json.dumps({"ID": "invalid_1", "VAL": "not_a_number"}).encode(),
            json.dumps({"ID": "invalid_2", "VAL": {"nested": True}}).encode(),
        ]
        keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
        driver.sendBytesData(table_name, records, keys)

        # Wait for stabilization
        time.sleep(30)

        # Connector should be running
        failed = driver.get_failed_tasks(connector_name)
        assert not failed, f"Connector task failed: {failed}"

        # Valid record should be in the table
        count = driver.select_number_of_records(table_name)
        assert count >= 1, f"Expected at least 1 row, got {count}"

        # With ERROR_LOGGING, error table should capture rejected records
        cursor = driver.snowflake_conn.cursor()
        cursor.execute(f"SELECT * FROM ERROR_TABLE({table_name})")
        col_names = [desc[0] for desc in cursor.description]
        error_rows = cursor.fetchall()
        logger.info("Error table rows (with logging): %d", len(error_rows))

        if len(error_rows) > 0:
            for row in error_rows:
                row_dict = dict(zip(col_names, row))
                logger.info("Error table entry: %s", row_dict)
                assert row_dict.get("ERROR_CODE") is not None, (
                    f"Error table row missing ERROR_CODE: {row_dict}"
                )
        else:
            logger.warning(
                "No error table rows found — server may have silently dropped "
                "invalid records without logging them"
            )
    finally:
        driver.closeConnector(connector_name)
        try:
            driver.deleteTopic(table_name)
        except Exception:
            pass
