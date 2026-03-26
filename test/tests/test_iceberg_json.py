"""E2E tests for Kafka Connector v4 iceberg JSON ingestion.

These tests are v4-only. V3 is excluded because:
  - v3 requires `snowflake.streaming.iceberg.enabled=true` in the connector config
    which the config migration does not add (v3 iceberg was experimental)
  - v3 had custom iceberg code (IcebergInitService, IcebergTableStreamingRecordMapper)
    that has been removed in v4
  - v4 uses SSv2 which handles iceberg tables transparently

Prerequisites:
  - An AWS external volume named `ICEBERG_EXTERNAL_VOLUME` must exist in the test
    Snowflake account.  The default is ``kafka_push_e2e_volume_aws``.  Override
    with the environment variable ``ICEBERG_EXTERNAL_VOLUME``.
"""

import json
import logging

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

_SAMPLE_MESSAGE = {
    "id": 1,
    "body_temperature": 36.6,
    "name": "Steve",
    "approved_coffee_types": ["Espresso", "Doppio", "Ristretto", "Lungo"],
    "animals_possessed": {"dogs": True, "cats": False},
}
RECORD_COUNT = 100


def _base_connector_config(topic: str, schematization: bool, validation: bool) -> dict:
    return {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": str(schematization).lower(),
        "snowflake.client.validation.enabled": str(validation).lower(),
        "topics": topic,
        "jmx": "true",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("schematization", [True, False], ids=["schema=on", "schema=off"])
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_iceberg_json_variant(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
    schematization: bool,
):
    """JSON ingestion into an iceberg table — full 2x2 matrix (validation x schematization).

    Matrix axes:
      - validation (compat=true / ht=false): controls whether the client-side
        RowValidator runs.
      - schematization (on/off): controls how the connector maps records to columns.

    ``schema=off`` (bag-of-bits): table has ``RECORD_METADATA VARIANT, RECORD_CONTENT
      VARIANT``.  Full JSON payload goes into RECORD_CONTENT.  Assertions use
      ``PARSE_JSON(RECORD_CONTENT):field::TYPE`` because iceberg stores VARIANT as a
      string-encoded JSON literal.

    ``schema=on`` (typed columns): table pre-declares all columns from the sample
      message — scalar fields as typed (ID NUMBER, BODY_TEMPERATURE FLOAT, NAME STRING)
      and complex fields as VARIANT (APPROVED_COFFEE_TYPES, ANIMALS_POSSESSED).
      RECORD_METADATA remains VARIANT.  No schema evolution is needed.
      Typed columns are accessed directly; VARIANT columns still need PARSE_JSON().

    V3 is excluded: v3 iceberg required ``snowflake.streaming.iceberg.enabled=true``
    which the config migration does not inject.
    """
    val_tag = "compat" if validation else "ht"
    sch_tag = "s1" if schematization else "s0"
    base_name = f"iceberg_jv_{val_tag}_{sch_tag}"

    if schematization:
        columns = (
            "(RECORD_METADATA VARIANT, "
            "ID BIGINT, "
            "BODY_TEMPERATURE DOUBLE, "
            "NAME TEXT, "
            "APPROVED_COFFEE_TYPES VARIANT, "
            "ANIMALS_POSSESSED VARIANT)"
        )
    else:
        columns = "(RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT)"

    table = create_iceberg_table(base_name.upper(), columns=columns, cleanup_topic=False)
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=_base_connector_config(topic, schematization=schematization, validation=validation)
    )
    driver.startConnectorWaitTime()

    records = [
        json.dumps(_SAMPLE_MESSAGE).encode("utf-8") for _ in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    if not schematization:
        # Bag-of-bits: full JSON payload is in RECORD_CONTENT VARIANT.
        # PARSE_JSON() is required because iceberg stores VARIANT as a
        # string-encoded JSON literal, not a parsed object.
        rows = table.select(
            "PARSE_JSON(RECORD_CONTENT):id::NUMBER            AS ID, "
            "PARSE_JSON(RECORD_CONTENT):body_temperature::FLOAT AS BODY_TEMPERATURE, "
            "PARSE_JSON(RECORD_CONTENT):name::STRING           AS NAME, "
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER        AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER     AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING         AS TOPIC, "
            "PARSE_JSON(RECORD_METADATA):SnowflakeConnectorPushTime::STRING AS PUSH_TIME",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row in the iceberg table"
        row = rows[0]
        assert row["ID"] == 1, f"Expected id=1, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"
        assert row["PUSH_TIME"] is not None, "Expected SnowflakeConnectorPushTime to be set"
    else:
        # Schematization=on: connector maps top-level JSON keys to pre-declared
        # typed columns.  Typed columns (ID, BODY_TEMPERATURE, NAME) are accessed
        # directly.  RECORD_METADATA is still VARIANT so needs PARSE_JSON().
        rows = table.select(
            '"ID", "BODY_TEMPERATURE", "NAME", '
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER        AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER     AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING         AS TOPIC, "
            "PARSE_JSON(RECORD_METADATA):SnowflakeConnectorPushTime::STRING AS PUSH_TIME",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row"
        row = rows[0]
        assert row["ID"] == 1, f"Expected id=1, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"
        assert row["PUSH_TIME"] is not None, "Expected SnowflakeConnectorPushTime to be set"


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_add_column(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Iceberg schema evolution — connector adds a new column mid-stream (client-side SE).

    The connector's internal SE mechanism issues ``ALTER ICEBERG TABLE ADD COLUMN``
    when it detects a column present in a record that is not yet in the table.

    Table starts with RECORD_METADATA VARIANT + CITY TEXT.  Wave 1 records carry
    ``{city, age}``: the connector's RowValidator detects AGE as new and issues
    ``ALTER ICEBERG TABLE ADD COLUMN``.  Wave 2 adds ``country``: SE adds COUNTRY.

    Uses ``validation=true`` (compat/client-side SE) so the RowValidator drives
    column additions.  Server-side SE (validation=false) does not support typed
    column additions on iceberg tables.
    """
    base_name = "iceberg_se_addcol"
    table = create_iceberg_table(
        base_name.upper(),
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=_base_connector_config(topic, schematization=True, validation=True)
    )
    driver.startConnectorWaitTime()

    wave1_count = 100
    driver.sendBytesData(
        topic,
        [json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8") for i in range(wave1_count)],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    # Verify connector SE added AGE column
    cols = {row[0] for row in table.schema()}
    assert "AGE" in cols, (
        f"Expected connector SE to add AGE column after wave 1, got: {cols}"
    )

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode("utf-8")
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    rows = table.select('"CITY", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW", f"Expected COUNTRY='TW', got {rows[0]['COUNTRY']!r}"

    null_country_count = table.select('COUNT(*) AS CNT', 'WHERE "COUNTRY" IS NULL')[0]["CNT"]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_multi_wave(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Iceberg SE — connector adds two successive new columns across three waves.

    Verifies that the connector's client-side SE can handle multiple sequential
    schema changes.  Each new column is added by the connector itself via
    ``ALTER ICEBERG TABLE ADD COLUMN`` (not by the test).

    Waves:
      1. Wave 1 (50 records): ``{city}`` — no SE needed.
      2. Wave 2 (50 records): ``{city, age}`` — connector SE adds AGE.
      3. Wave 3 (50 records): ``{city, age, country}`` — connector SE adds COUNTRY.

    After all waves:
      - Wave-1 rows: AGE IS NULL, COUNTRY IS NULL
      - Wave-2 rows: AGE set, COUNTRY IS NULL
      - Wave-3 rows: AGE set, COUNTRY set
    """
    base_name = "iceberg_se_multi"
    table = create_iceberg_table(
        base_name.upper(),
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=_base_connector_config(topic, schematization=True, validation=True)
    )
    driver.startConnectorWaitTime()

    wave1_count = 50
    driver.sendBytesData(
        topic,
        [json.dumps({"city": "Taipei"}).encode("utf-8") for _ in range(wave1_count)],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    wave3_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Kaohsiung", "age": 200 + i, "country": "TW"}).encode("utf-8")
            for i in range(wave3_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count + wave3_count)

    # Wave-1: AGE and COUNTRY both NULL
    w1_null = table.select(
        'COUNT(*) AS CNT',
        "WHERE \"CITY\" = 'Taipei' AND \"AGE\" IS NULL AND \"COUNTRY\" IS NULL",
    )[0]["CNT"]
    assert w1_null == wave1_count, (
        f"Expected {wave1_count} wave-1 rows with NULL AGE+COUNTRY, got {w1_null}"
    )

    # Wave-2: CITY='Hsinchu', AGE set, COUNTRY NULL
    w2_rows = table.select('"AGE", "COUNTRY"', "WHERE \"CITY\" = 'Hsinchu' LIMIT 1")
    assert w2_rows, "Expected at least one wave-2 row"
    assert w2_rows[0]["AGE"] is not None, "Expected AGE set for wave-2 rows"
    assert w2_rows[0]["COUNTRY"] is None, (
        f"Expected COUNTRY NULL for wave-2 rows, got {w2_rows[0]['COUNTRY']!r}"
    )

    # Wave-3: CITY='Kaohsiung', AGE set, COUNTRY='TW'
    w3_rows = table.select('"AGE", "COUNTRY"', "WHERE \"CITY\" = 'Kaohsiung' LIMIT 1")
    assert w3_rows, "Expected at least one wave-3 row"
    assert w3_rows[0]["AGE"] is not None, "Expected AGE set for wave-3 rows"
    assert w3_rows[0]["COUNTRY"] == "TW", (
        f"Expected COUNTRY='TW', got {w3_rows[0]['COUNTRY']!r}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Server-side SE (ENABLE_SCHEMA_EVOLUTION on the table, validation=false) "
        "silently discards typed (non-VARIANT) column additions on iceberg tables. "
        "Client-side SE (validation=true) does work after fixing the connector to "
        "issue ALTER ICEBERG TABLE ADD COLUMN, but this test exercises the HT path "
        "(validation=false) where server-side SE is the only mechanism.  Remove "
        "this xfail once Snowflake server-side SE supports typed columns on iceberg."
    ),
)
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_json(
    driver: KafkaDriver,
    name_salt: str,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """JSON schema evolution into an iceberg table (server-side SE, HT mode).

    Schema evolution on iceberg requires careful mode selection:
      - ICEBERG_VERSION = 3 is required for VARIANT (RECORD_METADATA).
      - Client-side SE uses ALTER TABLE ADD COLUMN, which ICEBERG_VERSION = 3
        tables reject for typed (non-VARIANT) columns.
      - Structured OBJECT (ICEBERG_VERSION < 3) fails SSv2 insertion with
        "Typed object schema mismatch in conversion".

    Resolution: use ``validation=false`` (HT mode) so client-side validation is
    never initialized.  Records flow directly to SSv2, which relies on
    ``ENABLE_SCHEMA_EVOLUTION = TRUE`` for server-side column additions.

    Sends two waves:
      1. Wave 1 (100 records): ``{city, age}`` — server-side SE adds CITY, AGE.
      2. Wave 2 (50 records): ``{city, age, country}`` — server-side SE adds COUNTRY.

    After both waves, verifies that all three columns exist and that wave-1 rows
    have NULL for COUNTRY.
    """
    base_name = "iceberg_se_json"
    table = create_iceberg_table(
        base_name.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config={
            # validation=False: avoids "Structured OBJECT types not supported
            # by Snowpipe Streaming" error from initializeValidation(), which
            # would disable the row validator and prevent client-side SE.
            # Server-side SE (ENABLE_SCHEMA_EVOLUTION on the table) handles
            # column additions instead.
            **_base_connector_config(topic, schematization=True, validation=False),
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.deadletterqueue.topic.name": f"DLQ_iceberg_se{name_salt}",
            "errors.deadletterqueue.topic.replication.factor": "1",
        }
    )
    driver.startConnectorWaitTime()

    wave1_count = 100
    driver.sendBytesData(
        topic,
        [json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8") for i in range(wave1_count)],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode("utf-8")
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    # Verify schema: all three columns must have been added
    cols = {row[0]: row[1] for row in table.schema()}
    assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
    assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    assert "COUNTRY" in cols, f"Expected COUNTRY column after wave 2, got: {list(cols.keys())}"

    # Wave-2 rows must have correct CITY and COUNTRY values
    rows = table.select('"CITY", "AGE", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW"

    # Wave-1 rows must have NULL COUNTRY
    null_country_count = table.select('COUNT(*) AS CNT', 'WHERE "COUNTRY" IS NULL')[0]["CNT"]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )
